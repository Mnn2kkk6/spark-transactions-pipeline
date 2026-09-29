"""
performance_lab.py
------------------
Bài tập: Performance trong PySpark.

Nội dung (mỗi phần là 1 BƯỚC trong file, chạy tuần tự, in kết quả đo được ra console):

  BƯỚC 1  Shuffle là gì, operation nào gây shuffle (narrow vs wide) -> đếm Exchange trong plan
  BƯỚC 2  Broadcast Join vs Sort-Merge Join: đo thời gian + đọc plan
  BƯỚC 3  Cache / Persist: chạy cùng 1 DataFrame nhiều action, có / không cache
  BƯỚC 4  Data Skew: phát hiện, đo "straggler task", xử lý bằng salting (join + aggregate),
          và thử AQE skew join
  BƯỚC 5  Đọc explain(): nhận biết Exchange / Sort / BroadcastExchange, đếm số stage

VÌ SAO KHÔNG DÙNG data/*.csv Ở BÀI NÀY?
  data/transactions.csv chỉ có ~106 dòng -> mọi phép đo thời gian / skew đều vô nghĩa
  (nhiễu lớn hơn tín hiệu). Bài này SINH DỮ LIỆU LỚN HƠN ngay trong Spark
  (spark.range, mặc định 1.000.000 transactions + 20.000 customers) với CÙNG schema
  logic (customer_id, amount, province...) như pipeline gốc. Không ghi/đè gì vào data/.

LƯU Ý KHI ĐỌC KẾT QUẢ:
  - Chạy local (`local[*]`), số liệu thời gian phụ thuộc máy và số core, chỉ có giá trị
    SO SÁNH TƯƠNG ĐỐI trong cùng 1 lần chạy, không phải benchmark tuyệt đối.
  - Data skew: local mode ít core thì không thấy hết "wall time" bị kéo dài, nên bài
    này đo trực tiếp thời gian TỪNG TASK (median vs max) từ Spark REST API để thấy straggler.

Chạy:  python performance_lab.py   (Windows)
       python3 performance_lab.py  (macOS/Linux)
Tuỳ chọn:  python3 performance_lab.py 300000   # đổi số transactions (mặc định 1_000_000)
"""

import contextlib
import io
import json
import re
import sys
import time
import urllib.request
from collections import Counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

N_TX = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000   # số transactions
N_CUST = 20_000                                                 # số customers
HOT_SHARE = 0.60        # 60% transactions của bản "lệch" dồn vào 1 customer duy nhất (hot key)
HOT_KEY = "C00000"
SALT_BUCKETS = 8        # số "salt" khi xử lý skew
SHUFFLE_PARTS = 8
PROVINCES = ["Hanoi", "HoChiMinh", "DaNang", "HaiPhong", "CanTho"]

spark = (
    SparkSession.builder
    .appName("performance-lab")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
    # Tắt AQE ở các bước 1-4 để plan "thô" đúng như lý thuyết (AQE có thể tự đổi
    # Sort-Merge Join thành Broadcast Join, tự gộp partition... làm che mất điều cần quan sát).
    # Bước 4d sẽ bật lại AQE để xem nó tự xử lý skew join thế nào.
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


# =====================================================================
# HÀM TIỆN ÍCH
# =====================================================================
def banner(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def plan_text(df):
    """Bắt output của explain(formatted) thành chuỗi."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


def plan_tree(df):
    """Chỉ phần cây plan (bỏ phần chi tiết từng node ở dưới)."""
    return plan_text(df).split("== Physical Plan ==")[-1].strip("\n").split("\n\n")[0]


def node_counts(df):
    """Đếm các node trong physical plan, vd {'Exchange': 2, 'Sort': 2, 'SortMergeJoin': 1}."""
    return Counter(re.findall(r"^\(\d+\) (\w+)", plan_text(df), flags=re.M))


def run(df):
    """Ép Spark thực thi TOÀN BỘ plan nhưng không tốn thời gian ghi ra đĩa (sink 'noop')."""
    df.write.format("noop").mode("overwrite").save()


def timed(df, repeat=1):
    """Chạy df `repeat` lần, trả về thời gian NHỎ NHẤT (giây) để giảm nhiễu."""
    best = None
    for _ in range(repeat):
        t0 = time.time()
        run(df)
        dt = time.time() - t0
        best = dt if best is None else min(best, dt)
    return best


def partition_sizes(df):
    """Số dòng trong từng partition của df (df đã được chia partition sẵn)."""
    rows = (
        df.groupBy(F.spark_partition_id().alias("pid")).count().orderBy("pid").collect()
    )
    return [r["count"] for r in rows]


def describe_sizes(sizes):
    total = sum(sizes)
    biggest = max(sizes)
    ordered = sorted(sizes)
    median = ordered[len(ordered) // 2]
    return (f"{sizes}\n      -> partition lớn nhất chứa {biggest:,}/{total:,} dòng "
            f"({biggest / total:.0%}); lớn nhất / median = {biggest / max(median, 1):.1f}x")


def task_stats(job_group):
    """
    Lấy thời gian từng task (median vs max) của các stage trong job group, qua Spark REST API.
    Trả về dict của stage có shuffle-read lớn nhất (thường là stage reduce/join/aggregate),
    hoặc None nếu không truy cập được UI.
    """
    try:
        tracker = sc.statusTracker()
        base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
        best = None
        for job_id in tracker.getJobIdsForGroup(job_group):
            for stage_id in tracker.getJobInfo(job_id).stageIds:
                url = f"{base}/stages/{stage_id}/0/taskSummary?quantiles=0.5,1.0"
                try:
                    d = json.load(urllib.request.urlopen(url, timeout=10))
                except Exception:      # stage bị skip / không có task (vd AQE) -> bỏ qua
                    continue
                if len(d.get("duration", [])) < 2:
                    continue
                read = d.get("shuffleReadMetrics", {}).get("readRecords", [0, 0])
                cand = {
                    "stage": stage_id,
                    "dur_median_ms": d["duration"][0], "dur_max_ms": d["duration"][1],
                    "read_median": read[0], "read_max": read[1],
                }
                if best is None or cand["read_max"] > best["read_max"]:
                    best = cand
        return best
    except Exception as exc:  # UI tắt / cổng khác / stage không có task summary
        print(f"   (không lấy được task stats từ Spark UI: {exc})")
        return None


def shuffle_totals(job_group):
    """Tổng dữ liệu shuffle-write (bytes, records) của mọi stage trong job group. Không phụ thuộc phần cứng."""
    try:
        tracker = sc.statusTracker()
        base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
        stage_ids = set()
        for job_id in tracker.getJobIdsForGroup(job_group):
            stage_ids.update(tracker.getJobInfo(job_id).stageIds)
        w_bytes = w_recs = 0
        for stage_id in stage_ids:
            d = json.load(urllib.request.urlopen(f"{base}/stages/{stage_id}/0", timeout=10))
            w_bytes += d.get("shuffleWriteBytes", 0)
            w_recs += d.get("shuffleWriteRecords", 0)
        return w_bytes, w_recs
    except Exception as exc:
        print(f"   (không lấy được shuffle metrics: {exc})")
        return None


def measure_shuffle(label, df):
    """Chạy df 1 lần trong 1 job group riêng và trả về tổng shuffle-write."""
    group = f"shuffle_{label}"
    sc.setJobGroup(group, label)
    run(df)
    return shuffle_totals(group)


def close_enough(a, b, rel=1e-9):
    """So sánh số thực có dung sai (cộng double theo thứ tự partition khác nhau cho sai số ở chữ số cuối)."""
    return abs(a - b) <= rel * max(abs(a), abs(b), 1.0)


def print_task_stats(label, stats):
    if not stats:
        return
    ratio_t = stats["dur_max_ms"] / max(stats["dur_median_ms"], 1)
    ratio_r = stats["read_max"] / max(stats["read_median"], 1)
    print(f"   [{label}] stage {stats['stage']}: thời gian task median={stats['dur_median_ms']:.0f}ms, "
          f"max={stats['dur_max_ms']:.0f}ms (x{ratio_t:.1f}) | "
          f"dòng shuffle-read median={stats['read_median']:,.0f}, max={stats['read_max']:,.0f} (x{ratio_r:.1f})")


# =====================================================================
# BƯỚC 0: SINH DỮ LIỆU LỚN (cùng "hình dạng" với pipeline gốc)
# =====================================================================
banner(f"BƯỚC 0: Sinh dữ liệu: {N_TX:,} transactions, {N_CUST:,} customers "
       f"(8 partition đầu vào; skew: {HOT_SHARE:.0%} dòng dồn vào {HOT_KEY})")

customers = spark.range(N_CUST).select(
    F.format_string("C%05d", F.col("id")).alias("customer_id"),
    F.concat(F.lit("Customer "), F.col("id").cast("string")).alias("customer_name"),
    F.element_at(F.array(*[F.lit(p) for p in PROVINCES]),
                 (F.col("id") % 5 + 1).cast("int")).alias("province"),
)

# customer_id phân bố ĐỀU: (id * 7919) % N_CUST  (7919 nguyên tố, nguyên tố cùng nhau với N_CUST)
uniform_customer_id = F.format_string("C%05d", (F.col("id") * 7919) % N_CUST)


def make_transactions(skewed):
    cid = (
        F.when(F.rand(seed=2) < HOT_SHARE, F.lit(HOT_KEY)).otherwise(uniform_customer_id)
        if skewed else uniform_customer_id
    )
    return spark.range(0, N_TX, 1, 8).select(
        F.col("id").alias("transaction_id"),
        cid.alias("customer_id"),
        (F.rand(seed=1) * 5_000_000 + 10_000).alias("amount"),
        F.element_at(F.array(F.lit("SUCCESS"), F.lit("FAILED"), F.lit("PENDING")),
                     (F.col("id") % 3 + 1).cast("int")).alias("status"),
    )


tx = make_transactions(skewed=False)          # phân bố đều
tx_skew = make_transactions(skewed=True)      # 1 hot key chiếm HOT_SHARE

print(f"tx (đều)   : {tx.rdd.getNumPartitions()} partition")
print(f"tx_skew    : {tx_skew.rdd.getNumPartitions()} partition")
print(f"customers  : {customers.rdd.getNumPartitions()} partition")

# khởi động JVM/JIT trước để lần đo đầu không bị "nóng máy" làm sai lệch
run(tx.groupBy("customer_id").count())

# =====================================================================
# BƯỚC 1: SHUFFLE - operation nào gây shuffle?
#   Shuffle = phân phối lại dữ liệu giữa các partition/executor qua đĩa + mạng.
#   Trong plan, mỗi shuffle hiện thành 1 node `Exchange` (hashpartitioning /
#   rangepartitioning / RoundRobinPartitioning / SinglePartition).
#   Narrow transformation (filter, select, withColumn, coalesce...): mỗi partition
#   con chỉ phụ thuộc 1 partition cha -> KHÔNG shuffle.
#   Wide transformation (groupBy, join, distinct, orderBy, window, repartition...): 1
#   partition con cần dữ liệu từ NHIỀU partition cha -> PHẢI shuffle.
# =====================================================================
banner("BƯỚC 1: Operation nào gây Shuffle? (đếm node trong physical plan)")

w_cust = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
hashed = tx.repartition("customer_id")

step1_cases = [
    ("filter + select + withColumn",     "narrow",
     tx.filter(F.col("amount") > 1000).select("customer_id", "amount")
       .withColumn("amount_k", F.col("amount") / 1000)),
    ("coalesce(2)",                      "narrow (gộp, không shuffle)", tx.coalesce(2)),
    ("repartition(8)",                   "wide",   tx.repartition(8)),
    ("groupBy(customer_id).agg(sum)",    "wide",
     tx.groupBy("customer_id").agg(F.sum("amount").alias("total"))),
    ("distinct(customer_id)",            "wide",   tx.select("customer_id").distinct()),
    ("orderBy(amount)",                  "wide",   tx.orderBy("amount")),
    ("Window row_number() by customer",  "wide",
     tx.withColumn("rn", F.row_number().over(w_cust))),
    ("join - Sort-Merge (hint merge)",   "wide (2 phía đều shuffle)",
     tx.join(customers.hint("merge"), "customer_id")),
    ("join - Broadcast (bảng nhỏ)",      "wide nhưng KHÔNG shuffle bảng lớn",
     tx.join(F.broadcast(customers), "customer_id")),
    ("repartition(cid) rồi groupBy(cid)", "shuffle 1 lần, dùng lại",
     hashed.groupBy("customer_id").agg(F.sum("amount").alias("total"))),
]

print(f"{'Case':<38}{'Loại':<36}{'Exchange':>9}{'Broadcast':>10}{'Sort':>6}")
print("-" * 99)
for name, kind, df_case in step1_cases:
    c = node_counts(df_case)
    print(f"{name:<38}{kind:<36}{c['Exchange']:>9}{c['BroadcastExchange']:>10}{c['Sort']:>6}")

print("""
Nhận xét:
 - narrow (filter/select/withColumn/coalesce): 0 Exchange.
 - groupBy / distinct / orderBy / Window / repartition: mỗi cái 1 Exchange. groupBy+agg còn
   được Spark tách thành HashAggregate(partial) -> Exchange -> HashAggregate(final): gom sơ bộ
   ở từng partition TRƯỚC khi shuffle để giảm lượng dữ liệu phải chuyển.
 - Sort-Merge Join: 2 Exchange (shuffle CẢ HAI bảng theo khoá join) + 2 Sort.
 - Broadcast Join: 0 Exchange cho bảng lớn; chỉ có BroadcastExchange (gửi bảng nhỏ tới mọi executor).
 - repartition(cid) rồi groupBy(cid): chỉ 1 Exchange, vì dữ liệu ĐÃ được chia theo cid nên groupBy
   không cần shuffle lần nữa -> repartition sớm theo key hữu ích khi có nhiều bước cùng key.""")

# Ảnh hưởng của số shuffle partition tới kết quả groupBy nhỏ (minh hoạ tham số quan trọng nhất)
print("Số partition sau shuffle = spark.sql.shuffle.partitions "
      f"(đang đặt {SHUFFLE_PARTS}; mặc định của Spark là 200).")
agg_demo = tx.groupBy("customer_id").agg(F.sum("amount").alias("total"))
print(f"  groupBy -> số partition kết quả: {agg_demo.rdd.getNumPartitions()}")

# =====================================================================
# BƯỚC 2: BROADCAST JOIN vs SORT-MERGE JOIN
# =====================================================================
banner("BƯỚC 2: Broadcast Join vs Sort-Merge Join")

# 2a. Spark tự chọn gì nếu KHÔNG hint? (customers nhỏ hơn autoBroadcastJoinThreshold = 10MB)
auto_join = tx.join(customers, "customer_id")
thr = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"spark.sql.autoBroadcastJoinThreshold = {thr}")
print(f"Join KHÔNG hint -> plan: {dict(node_counts(auto_join))}")

smj_join = tx.join(customers.hint("merge"), "customer_id")
bhj_join = tx.join(F.broadcast(customers), "customer_id")

print("\n--- Plan SORT-MERGE JOIN (2 Exchange + 2 Sort: shuffle cả 2 bảng) ---")
print(plan_tree(smj_join))
print("\n--- Plan BROADCAST HASH JOIN (BroadcastExchange, không Exchange cho bảng lớn) ---")
print(plan_tree(bhj_join))

# 2b. Đo thời gian (best of 2)
t_smj = timed(smj_join, repeat=2)
t_bhj = timed(bhj_join, repeat=2)
print(f"\nThời gian join {N_TX:,} tx x {N_CUST:,} customers (best of 2):")
print(f"  Sort-Merge Join : {t_smj:6.2f}s   (shuffle + sort cả 2 bảng)")
print(f"  Broadcast Join  : {t_bhj:6.2f}s   (không shuffle bảng lớn)")
print(f"  => Broadcast nhanh hơn {t_smj / t_bhj:.1f}x  (thời gian phụ thuộc máy, xem chỉ số shuffle bên dưới)")

# 2c. Lượng dữ liệu phải shuffle (chỉ số không phụ thuộc phần cứng)
sh_smj = measure_shuffle("smj", smj_join)
sh_bhj = measure_shuffle("bhj", bhj_join)
if sh_smj and sh_bhj:
    print("\nDữ liệu shuffle-write của cả job (từ Spark REST API):")
    print(f"  Sort-Merge Join : {sh_smj[0] / 1024 / 1024:8.1f} MB, {sh_smj[1]:>10,} records")
    print(f"  Broadcast Join  : {sh_bhj[0] / 1024 / 1024:8.1f} MB, {sh_bhj[1]:>10,} records")

# 2d. Kiểm tra kết quả giống nhau (tối ưu hoá không được làm đổi kết quả)
chk_smj = smj_join.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).first()
chk_bhj = bhj_join.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).first()
assert chk_smj["n"] == chk_bhj["n"] and close_enough(chk_smj["s"], chk_bhj["s"]), "2 kiểu join phải cho cùng kết quả"
print(f"  [OK] cùng kết quả: {chk_bhj['n']:,} dòng, tổng amount = {chk_bhj['s']:,.2f}")

print("""
Khi nào dùng / không dùng Broadcast Join:
 - Dùng khi 1 bên đủ nhỏ để nằm gọn trong bộ nhớ của driver + MỌI executor (mặc định < 10MB,
   chỉnh bằng spark.sql.autoBroadcastJoinThreshold hoặc ép bằng F.broadcast()/hint("broadcast")).
 - Không dùng khi bảng "nhỏ" thực ra lớn: broadcast tốn bộ nhớ driver (phải collect về trước)
   + nhân bản ra mọi executor -> dễ OOM. Khi đó Sort-Merge Join hợp lý hơn.
 - Không broadcast được bên "được giữ lại" của outer join (vd left join thì không broadcast
   được bảng bên trái) - bảng nhỏ phải là bên còn lại.
 - Broadcast Join còn TRÁNH được skew phía join: bảng lớn không bị shuffle theo key.""")

# =====================================================================
# BƯỚC 3: CACHE / PERSIST
#   Mặc định Spark KHÔNG lưu kết quả trung gian: mỗi action tính lại toàn bộ lineage từ đầu.
#   cache()/persist() giữ lại kết quả sau lần tính đầu để các action sau dùng lại.
#   Lưu ý: cache là LAZY (chỉ đánh dấu), lần action đầu tiên mới thật sự nạp cache.
# =====================================================================
banner("BƯỚC 3: Cache / Persist")


def build_expensive():
    """DataFrame 'tốn kém': join Sort-Merge (shuffle 2 bảng) + groupBy (shuffle nữa)."""
    return (
        tx.join(customers.hint("merge"), "customer_id")
        .groupBy("customer_id", "province")
        .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("n_tx"))
    )


def three_actions(df):
    """3 action khác nhau cùng dùng lại `df` (điển hình: xem count, tổng hợp, top-N)."""
    times = []
    for label, action in [
        ("count()", lambda: df.count()),
        ("tổng theo province", lambda: df.groupBy("province").agg(F.sum("total_amount")).collect()),
        ("top 10 customer", lambda: df.orderBy(F.col("total_amount").desc()).limit(10).collect()),
    ]:
        t0 = time.time()
        action()
        times.append((label, time.time() - t0))
    return times


print("--- KHÔNG cache: mỗi action tính lại join + groupBy từ đầu ---")
no_cache_times = three_actions(build_expensive())
for label, dt in no_cache_times:
    print(f"   {label:<22}{dt:6.2f}s")
total_no_cache = sum(dt for _, dt in no_cache_times)
print(f"   {'TỔNG':<22}{total_no_cache:6.2f}s")

print("\n--- CÓ cache: action đầu tiên nạp cache, các action sau đọc từ bộ nhớ ---")
cached_df = build_expensive()
t0 = time.time()
cached_df.cache()
print(f"   gọi .cache()          {time.time() - t0:6.2f}s   <- gần 0: cache là lazy, chưa tính gì")
print(f"   is_cached = {cached_df.is_cached}, storageLevel sau cache(): {cached_df.storageLevel}")
cache_times = three_actions(cached_df)
for label, dt in cache_times:
    print(f"   {label:<22}{dt:6.2f}s")
total_cache = sum(dt for _, dt in cache_times)
print(f"   {'TỔNG':<22}{total_cache:6.2f}s")
print(f"   => Có cache nhanh hơn {total_no_cache / total_cache:.1f}x "
      f"(action đầu vẫn trả giá tính toán, các action sau gần như miễn phí)")

cached_plan = plan_text(cached_df.groupBy("province").agg(F.sum("total_amount")))
assert "InMemoryTableScan" in cached_plan
print("\nPlan khi đã cache: query mới đọc từ InMemoryTableScan (các node bên dưới InMemoryRelation")
print("chỉ là plan đã dùng để NẠP cache, không chạy lại):")
print(plan_tree(cached_df.groupBy("province").agg(F.sum("total_amount"))))

cached_df.unpersist(blocking=True)
print(f"\nSau unpersist(): is_cached = {cached_df.is_cached}  (nhả bộ nhớ khi hết dùng)")

# persist() với storage level tường minh
persisted_df = build_expensive().persist(StorageLevel.MEMORY_AND_DISK)
persisted_df.count()
print(f"persist(MEMORY_AND_DISK): storageLevel = {persisted_df.storageLevel}")
persisted_df.unpersist(blocking=True)

print("""
Khi nào nên / không nên cache:
 - NÊN: DataFrame tốn kém (nhiều shuffle/join) và được DÙNG LẠI >= 2 action / nhiều nhánh xử lý.
 - KHÔNG NÊN: chỉ dùng 1 lần (cache thêm overhead, không lợi gì); dữ liệu quá lớn so với
   bộ nhớ (bị đẩy ra đĩa hoặc bị evict); hoặc quên unpersist() làm chiếm bộ nhớ cả job.
 - persist(level) cho phép chọn MEMORY_ONLY / MEMORY_AND_DISK / DISK_ONLY...;
   cache() = persist() với level mặc định.
 - Cache chỉ tồn tại trong 1 Spark application, không thay thế việc ghi checkpoint/Parquet.""")

# =====================================================================
# BƯỚC 4: DATA SKEW
#   Skew = dữ liệu phân bố không đều theo key -> sau shuffle theo key, vài partition
#   chứa quá nhiều dòng. Stage chỉ xong khi task CHẬM NHẤT xong (straggler), các
#   core khác ngồi chờ; partition quá lớn còn dễ gây OOM / spill ra đĩa.
# =====================================================================
banner("BƯỚC 4a: Phát hiện skew")

key_dist = (
    tx_skew.groupBy("customer_id").count().orderBy(F.col("count").desc()).limit(5).collect()
)
print(f"Top 5 customer_id theo số transaction (tx_skew, tổng {N_TX:,}):")
for r in key_dist:
    print(f"   {r['customer_id']}: {r['count']:>9,}  ({r['count'] / N_TX:.1%})")
print("Bình thường mỗi customer có ~", N_TX // N_CUST, "dòng -> hot key gấp hàng trăm lần.")

print("\nSố dòng mỗi partition sau khi chia theo customer_id (đây là thứ Sort-Merge Join / groupBy sẽ thấy):")
print("   tx (đều)  :", describe_sizes(partition_sizes(tx.repartition(SHUFFLE_PARTS, "customer_id"))))
print("   tx_skew   :", describe_sizes(partition_sizes(tx_skew.repartition(SHUFFLE_PARTS, "customer_id"))))

banner("BƯỚC 4b: Skew làm task chậm bất thường (đo thời gian từng task)")

sc.setJobGroup("join_uniform", "join du lieu deu")
run(tx.join(customers.hint("merge"), "customer_id"))
stats_uniform = task_stats("join_uniform")

sc.setJobGroup("join_skew", "join du lieu lech")
t_skew_join = timed(tx_skew.join(customers.hint("merge"), "customer_id"))
stats_skew = task_stats("join_skew")

print("Sort-Merge Join theo customer_id, stage reduce (đọc shuffle):")
print_task_stats("dữ liệu ĐỀU ", stats_uniform)
print_task_stats("dữ liệu LỆCH ", stats_skew)
print("=> Trong 1 stage, thời gian stage ~ thời gian task CHẬM NHẤT. Ở cluster nhiều core, "
      "các core còn lại xong sớm rồi ngồi chờ 1 task straggler.")

banner("BƯỚC 4c: Xử lý skew bằng SALTING")

# --- (i) Aggregate: 2 giai đoạn (phần 1 rải hot key qua nhiều salt, phần 2 gộp lại) ---
salted_tx = tx_skew.withColumn("salt", F.floor(F.rand(seed=3) * SALT_BUCKETS).cast("int"))

direct_agg = tx_skew.groupBy("customer_id").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
two_stage_agg = (
    salted_tx.groupBy("customer_id", "salt")                      # giai đoạn 1: hot key bị chia SALT_BUCKETS mảnh
    .agg(F.sum("amount").alias("part_total"), F.count("*").alias("part_n"))
    .groupBy("customer_id")                                        # giai đoạn 2: gộp lại (chỉ còn <= SALT_BUCKETS dòng/key)
    .agg(F.sum("part_total").alias("total"), F.sum("part_n").alias("n"))
)

print("(i) Aggregate với salting 2 giai đoạn")
print("   Phân bố dòng theo partition nếu shuffle theo key:")
print("     customer_id          :", describe_sizes(partition_sizes(tx_skew.repartition(SHUFFLE_PARTS, "customer_id"))))
print("     (customer_id, salt)  :", describe_sizes(partition_sizes(salted_tx.repartition(SHUFFLE_PARTS, "customer_id", "salt"))))

print("   Số salt bucket ảnh hưởng độ đều thế nào (shuffle 8 partition; hash có thể đụng nhau nên không bao giờ đều tuyệt đối):")
for sb in (8, 16, 32):
    d = tx_skew.withColumn("salt", F.floor(F.rand(seed=3) * sb).cast("int")).repartition(SHUFFLE_PARTS, "customer_id", "salt")
    sizes = partition_sizes(d)
    print(f"     {sb:>2} salt bucket: partition lớn nhất chiếm {max(sizes) / N_TX:.0%} tổng số dòng"
          f"  (đổi lại bảng nhỏ phải nhân bản x{sb})")

# kiểm tra kết quả phải trùng khớp
cmp = (
    direct_agg.alias("a").join(two_stage_agg.alias("b"), "customer_id")
    .filter((F.abs(F.col("a.total") - F.col("b.total")) > 0.01 * F.col("a.n")) | (F.col("a.n") != F.col("b.n")))
    .count()
)
assert cmp == 0, "kết quả aggregate sau salting phải khớp kết quả gốc"
print(f"   [OK] kết quả aggregate sau salting KHỚP kết quả gốc trên cả {N_CUST:,} customer")

# --- (ii) Join: salt phía bảng lớn, nhân bản bảng nhỏ theo salt ---
salt_values = spark.range(SALT_BUCKETS).select(F.col("id").cast("int").alias("salt"))
customers_salted = customers.crossJoin(salt_values)   # nhân bản bảng nhỏ SALT_BUCKETS lần
print(f"\n(ii) Join với salting: bảng nhỏ nhân bản x{SALT_BUCKETS} "
      f"({N_CUST:,} -> {N_CUST * SALT_BUCKETS:,} dòng), join theo (customer_id, salt)")

skew_join = tx_skew.join(customers.hint("merge"), "customer_id")
salted_join = salted_tx.join(customers_salted.hint("merge"), ["customer_id", "salt"])

sc.setJobGroup("join_salted", "join sau salting")
t_salted_join = timed(salted_join)
stats_salted = task_stats("join_salted")
print("   Stage reduce của Sort-Merge Join:")
print_task_stats("LỆCH, chưa salt", stats_skew)
print_task_stats("LỆCH, đã salt   ", stats_salted)
print(f"   Tổng thời gian trên máy local này: chưa salt {t_skew_join:.2f}s | đã salt {t_salted_join:.2f}s")
print("   LƯU Ý: thời gian (ms) của từng task dao động khá nhiều giữa các lần chạy trên máy ít core, đừng tin 1 con số.\n"
      "   Chỉ số ỔN ĐỊNH và đáng tin là SỐ DÒNG mà task lớn nhất phải xử lý (cột shuffle-read max).\n"
      "   Tổng thời gian local có thể không giảm (salting còn tốn thêm do nhân bản bảng nhỏ); lợi ích thật xuất hiện\n"
      "   trên cluster nhiều core, nơi thời gian stage ~ thời gian task chậm nhất.")

s1 = skew_join.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).first()
s2 = salted_join.agg(F.count("*").alias("n"), F.sum("amount").alias("s")).first()
assert s1["n"] == s2["n"] and close_enough(s1["s"], s2["s"]), "join có salt phải cho cùng kết quả"
print(f"   [OK] join sau salting cho cùng kết quả: {s2['n']:,} dòng, tổng amount = {s2['s']:,.2f}")

print("""
Các cách xử lý skew khác (không chạy trong bài này):
 - Broadcast Join nếu 1 bên đủ nhỏ (tránh shuffle bảng lớn -> hết skew phía join).
 - Tách riêng hot key: xử lý hot key bằng broadcast / logic riêng, phần còn lại join bình thường.
 - Lọc key rác trước khi join (vd customer_id null/rỗng dồn về 1 partition).
 - Bật AQE skew join (bước 4d): Spark tự chia partition quá lớn thành nhiều mảnh.""")

banner("BƯỚC 4d: AQE skew join (Spark tự xử lý skew lúc runtime)")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Ngưỡng mặc định (partition > 256MB và > 5x median) quá lớn với dataset bài lab nên hạ xuống
# để AQE nhận ra skew. Trên dữ liệu thật giữ mặc định hoặc chỉnh theo cluster.
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "2MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # ép Sort-Merge Join để AQE có thứ để tối ưu

aqe_query = (
    tx_skew.join(customers, "customer_id")
    .groupBy("province").agg(F.count("*").alias("n"), F.sum("amount").alias("total"))
)
sc.setJobGroup("join_aqe", "aqe skew join")
aqe_result = aqe_query.collect()          # phải chạy xong thì explain() mới hiện plan CUỐI CÙNG của AQE
aqe_plan = plan_text(aqe_query)
aqe_skew_on = "skew=true" in aqe_plan
print("Plan cuối cùng của AQE có node `SortMergeJoin(skew=true)`:", aqe_skew_on)
print("   ", "AQE đã phát hiện partition lệch và chia nhỏ nó thành nhiều task." if aqe_skew_on
      else "AQE không kích hoạt skew join với ngưỡng hiện tại.")
for line in aqe_plan.splitlines():
    if re.search(r"AQEShuffleRead|skew", line) and re.match(r"^\(\d+\)|^Arguments", line):
        print("      ", line.strip())
stats_aqe = task_stats("join_aqe")
print_task_stats("AQE bật      ", stats_aqe)
assert sum(r["n"] for r in aqe_result) == N_TX

# trả lại cấu hình cho các bước sau
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# =====================================================================
# BƯỚC 5: ĐỌC explain()
# =====================================================================
banner("BƯỚC 5: Đọc explain() của 1 query đầy đủ")

final_query = (
    tx.join(customers, "customer_id")                       # bảng nhỏ -> tự broadcast
    .filter(F.col("amount") > 100_000)
    .groupBy("province")
    .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("n_tx"))
    .orderBy(F.col("total_amount").desc())
)

print("Cây plan (đọc từ DƯỚI lên TRÊN = thứ tự thực thi; thụt vào nhiều hơn = chạy trước):")
print(plan_tree(final_query))

counts = node_counts(final_query)
n_shuffle = counts["Exchange"]
print(f"\nĐếm node: {dict(counts)}")

legend = {
    "Range": "nguồn dữ liệu (ở pipeline thật là Scan csv/parquet)",
    "Filter": "narrow: lọc từng dòng, không shuffle",
    "Project": "narrow: chọn / tính cột, không shuffle",
    "BroadcastExchange": "gửi bảng nhỏ tới mọi executor (KHÔNG phải shuffle bảng lớn)",
    "BroadcastHashJoin": "join bằng hash table của bảng đã broadcast",
    "HashAggregate": "aggregate; xuất hiện 2 lần = partial (trước shuffle) + final (sau shuffle)",
    "Exchange": "SHUFFLE - ranh giới stage, tốn đĩa + mạng",
    "Sort": "sắp xếp trong partition (cho ORDER BY / Sort-Merge Join / Window)",
    "SortMergeJoin": "join 2 bảng đã shuffle + sort theo khoá",
    "Window": "tính window function trên partition đã shuffle + sort",
    "InMemoryTableScan": "đọc từ cache thay vì tính lại",
}
print("\nÝ nghĩa các node có trong plan này:")
for name in counts:
    if name in legend:
        print(f"   {name:<20} {legend[name]}")

# Đối chiếu với thực tế: chạy query và đếm job / stage
sc.setJobGroup("final_query", "explain demo")
final_query.collect()
tracker = sc.statusTracker()
print(f"\nSố Exchange (shuffle) trong plan = {n_shuffle}  ->  job chính có ~ {n_shuffle + 1} stage (mỗi shuffle cắt 1 stage).")
print("Thực tế Spark chạy nhiều JOB cho 1 action, mỗi job có số stage riêng:")
job_rows = []
for jid in sorted(tracker.getJobIdsForGroup("final_query")):
    ji = tracker.getJobInfo(jid)
    job_rows.append((jid, len(ji.stageIds)))
    print(f"   job {jid}: {len(ji.stageIds)} stage")
print("   (job có ít stage hơn thường là job build BroadcastExchange và job lấy mẫu chia range cho orderBy; job nhiều stage nhất là job chính)")
assert max(n for _, n in job_rows) == n_shuffle + 1, "job chính phải có (số Exchange + 1) stage"
print(f"   [OK] job chính có {n_shuffle + 1} stage = số Exchange + 1")

print("""
Cách đọc nhanh 1 plan khi tối ưu:
 1. Đếm Exchange: càng nhiều càng tốn; tự hỏi có bỏ bớt / dùng lại được không
    (vd repartition sớm theo key chung, broadcast bảng nhỏ, gộp bước groupBy).
 2. Thấy SortMergeJoin + 2 Exchange + 2 Sort mà 1 bên nhỏ -> cân nhắc F.broadcast().
 3. Exchange hashpartitioning(key, N): kiểm tra key có bị skew không, N (shuffle.partitions) có hợp lý không.
 4. Exchange rangepartitioning + Sort ở cuối = ORDER BY toàn cục; nếu không cần thứ tự toàn cục
    thì bỏ orderBy hoặc dùng sortWithinPartitions.
 5. Cache xong mà plan vẫn không có InMemoryTableScan -> cache chưa được dùng (chưa action nào nạp cache
    hoặc plan dùng nhánh khác).""")

# =====================================================================
# TỔNG KẾT
# =====================================================================
banner("TỔNG KẾT SỐ LIỆU")
print(f"Dữ liệu: {N_TX:,} transactions x {N_CUST:,} customers")
print(f"Join      : Sort-Merge {t_smj:.2f}s vs Broadcast {t_bhj:.2f}s  ->  Broadcast nhanh hơn {t_smj / t_bhj:.1f}x")
print(f"Cache     : 3 action không cache {total_no_cache:.2f}s vs có cache {total_cache:.2f}s  ->  nhanh hơn {total_no_cache / total_cache:.1f}x")
if stats_skew and stats_uniform:
    print("Data skew : task chậm nhất của stage join (ms) - số tuyệt đối dao động giữa các lần chạy, xem xu hướng:")
    print(f"            dữ liệu đều {stats_uniform['dur_max_ms']:.0f}ms | lệch {stats_skew['dur_max_ms']:.0f}ms"
          + (f" | lệch + salting {stats_salted['dur_max_ms']:.0f}ms" if stats_salted else "")
          + (f" | lệch + AQE skew join {stats_aqe['dur_max_ms']:.0f}ms" if stats_aqe else ""))
    print(f"            dòng shuffle-read của task lớn nhất: đều {stats_uniform['read_max']:,.0f} | lệch {stats_skew['read_max']:,.0f}"
          + (f" | salting {stats_salted['read_max']:,.0f}" if stats_salted else "")
          + (f" | AQE {stats_aqe['read_max']:,.0f}" if stats_aqe else ""))

spark.stop()
print("\nDONE.")
