"""
pipeline.py
-----------
Flow:  raw data -> validate -> deduplicate -> join -> window -> aggregate -> partitioned output

Chạy:  python3 pipeline.py
"""

import shutil
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = f"{BASE}/data"
OUT_DIR = f"{BASE}/output"

# dọn output cũ để chạy lại nhiều lần cho sạch
if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
os.makedirs(OUT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("transactions-pipeline")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")  # dataset nhỏ, giảm số partition mặc định (200) cho gọn
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================================
# BƯỚC 1: Đọc dữ liệu bằng schema khai báo sẵn (KHÔNG dùng inferSchema)
# =====================================================================
customers_schema = StructType([
    StructField("customer_id",   StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("province",      StringType(), True),
    StructField("created_at",    TimestampType(), True),
])

transactions_schema = StructType([
    StructField("transaction_id",    StringType(), True),
    StructField("customer_id",       StringType(), True),
    StructField("amount",            DoubleType(), True),
    StructField("status",            StringType(), True),
    StructField("transaction_time",  TimestampType(), True),
    StructField("updated_at",        TimestampType(), True),
])

customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv(f"{DATA_DIR}/customers.csv")
)

transactions_raw_df = (
    spark.read
    .option("header", True)
    .schema(transactions_schema)
    .csv(f"{DATA_DIR}/transactions.csv")
)

print("=== customers_df schema ===")
customers_df.printSchema()
print("=== transactions_raw_df schema ===")
transactions_raw_df.printSchema()
print(f"Raw transactions count: {transactions_raw_df.count()}")

# =====================================================================
# BƯỚC 2: Validate -> tách record lỗi
#   - thiếu customer_id (null hoặc rỗng)
#   - amount <= 0
# =====================================================================
is_missing_customer = F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")
is_invalid_amount = F.col("amount").isNull() | (F.col("amount") <= 0)

bad_records_df = transactions_raw_df.filter(is_missing_customer | is_invalid_amount) \
    .withColumn(
        "error_reason",
        F.when(is_missing_customer & is_invalid_amount, F.lit("MISSING_CUSTOMER_ID;INVALID_AMOUNT"))
         .when(is_missing_customer, F.lit("MISSING_CUSTOMER_ID"))
         .otherwise(F.lit("INVALID_AMOUNT"))
    )

clean_records_df = transactions_raw_df.filter(~(is_missing_customer | is_invalid_amount))

print(f"Bad records (missing customer_id / amount<=0): {bad_records_df.count()}")
print(f"Clean records (đủ điều kiện đi tiếp): {clean_records_df.count()}")

# =====================================================================
# BƯỚC 3: transaction_id bị trùng -> dùng Window Function
#   giữ bản ghi có updated_at MỚI NHẤT cho mỗi transaction_id
# =====================================================================
dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())

deduped_df = (
    clean_records_df
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

n_before_dedup = clean_records_df.count()
n_after_dedup = deduped_df.count()
print(f"Trước dedup: {n_before_dedup} | Sau dedup (giữ updated_at mới nhất): {n_after_dedup}"
      f" | Số bản ghi trùng bị loại: {n_before_dedup - n_after_dedup}")

# =====================================================================
# BƯỚC 4: Left join transactions (deduped) với customers
# =====================================================================
joined_df = deduped_df.join(customers_df, on="customer_id", how="left")

# Transaction không map được customer (orphan customer_id, không tồn tại trong customers)
unmapped_df = joined_df.filter(F.col("customer_name").isNull())
mapped_df = joined_df.filter(F.col("customer_name").isNotNull())

print(f"Unmapped (customer_id không tồn tại trong customers): {unmapped_df.count()}")
print(f"Mapped thành công: {mapped_df.count()}")

# =====================================================================
# BƯỚC 5: Với mỗi customer, dùng Window Function tìm transaction GẦN NHẤT
#   (theo transaction_time mới nhất)
# =====================================================================
latest_tx_window = Window.partitionBy("customer_id").orderBy(F.col("transaction_time").desc())

mapped_with_latest_flag_df = mapped_df.withColumn(
    "rn_latest", F.row_number().over(latest_tx_window)
)

latest_tx_per_customer_df = (
    mapped_with_latest_flag_df
    .filter(F.col("rn_latest") == 1)
    .select(
        "customer_id", "customer_name", "province",
        F.col("transaction_id").alias("latest_transaction_id"),
        F.col("amount").alias("latest_amount"),
        F.col("status").alias("latest_status"),
        F.col("transaction_time").alias("latest_transaction_time"),
    )
)

print("=== Transaction gần nhất của mỗi customer (5 dòng đầu) ===")
latest_tx_per_customer_df.orderBy("customer_id").show(5, truncate=False)

# =====================================================================
# BƯỚC 6: Tổng hợp theo customer
#   - tổng số transaction
#   - tổng amount
#   - số transaction SUCCESS
# =====================================================================
customer_agg_df = (
    mapped_df.groupBy("customer_id", "customer_name", "province")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
)

# gộp thêm cột transaction gần nhất để có full picture cho từng customer
customer_summary_df = customer_agg_df.join(
    latest_tx_per_customer_df.select(
        "customer_id", "latest_transaction_id", "latest_amount",
        "latest_status", "latest_transaction_time"
    ),
    on="customer_id",
    how="left",
)

print("=== Customer summary (tổng số tx, tổng amount, success_count, tx gần nhất) ===")
customer_summary_df.orderBy("customer_id").show(20, truncate=False)

# =====================================================================
# BƯỚC 7: Top 3 customer có tổng amount cao nhất theo TỪNG PROVINCE
# =====================================================================
province_rank_window = Window.partitionBy("province").orderBy(F.col("total_amount").desc())

top3_by_province_df = (
    customer_agg_df
    .withColumn("rank_in_province", F.rank().over(province_rank_window))
    .filter(F.col("rank_in_province") <= 3)
    .orderBy("province", "rank_in_province")
)

print("=== Top 3 customer theo total_amount, từng province ===")
top3_by_province_df.show(50, truncate=False)

# =====================================================================
# BƯỚC 8: Ghi dữ liệu hợp lệ ra Parquet, partition theo province
#   ("hợp lệ" = mapped_df: đã qua validate + dedup + join thành công)
# =====================================================================
valid_output_path = f"{OUT_DIR}/valid_transactions_parquet"
mapped_df.write.mode("overwrite").partitionBy("province").parquet(valid_output_path)

# =====================================================================
# BƯỚC 9: Ghi dữ liệu lỗi / unmapped ra output riêng
# =====================================================================
error_output_path = f"{OUT_DIR}/error_records_parquet"
unmapped_output_path = f"{OUT_DIR}/unmapped_records_parquet"

bad_records_df.write.mode("overwrite").parquet(error_output_path)
unmapped_df.write.mode("overwrite").parquet(unmapped_output_path)

# Ngoài ra ghi thêm các bảng tổng hợp để tiện xem lại (không bắt buộc nhưng hữu ích)
customer_summary_df.write.mode("overwrite").parquet(f"{OUT_DIR}/customer_summary_parquet")
top3_by_province_df.write.mode("overwrite").parquet(f"{OUT_DIR}/top3_by_province_parquet")

# =====================================================================
# BƯỚC 10: Đọc lại output và kiểm tra count (verify ghi/đọc đúng)
# =====================================================================
print("\n================= VERIFY (đọc lại output) =================")

valid_read_back = spark.read.parquet(valid_output_path)
error_read_back = spark.read.parquet(error_output_path)
unmapped_read_back = spark.read.parquet(unmapped_output_path)

print(f"[valid_transactions_parquet]  written={mapped_df.count():>4}  read_back={valid_read_back.count():>4}"
      f"  match={mapped_df.count() == valid_read_back.count()}")
print(f"[error_records_parquet]       written={bad_records_df.count():>4}  read_back={error_read_back.count():>4}"
      f"  match={bad_records_df.count() == error_read_back.count()}")
print(f"[unmapped_records_parquet]    written={unmapped_df.count():>4}  read_back={unmapped_read_back.count():>4}"
      f"  match={unmapped_df.count() == unmapped_read_back.count()}")

# Kiểm tra tổng: raw = clean(valid+error 2 loại) ... để chắc không rơi rớt record
total_check = bad_records_df.count() + n_after_dedup  # bad + deduped-clean phải nối lại được
print(f"\nRaw transactions: {transactions_raw_df.count()}"
      f" | Duplicate rows removed: {n_before_dedup - n_after_dedup}"
      f" | Bad(validate-fail): {bad_records_df.count()}"
      f" | Sau dedup+valid: {n_after_dedup}"
      f" (= mapped {mapped_df.count()} + unmapped {unmapped_df.count()})")

# List các partition thư mục province thực tế được tạo ra
print("\nCác partition (province) đã ghi:")
for p in sorted(os.listdir(valid_output_path)):
    if p.startswith("province="):
        print("  -", p)

# =====================================================================
# BƯỚC 11: explain() để xem execution plan (join / shuffle / sort)
# =====================================================================
print("\n================= EXPLAIN: dedup window (T00025... duplicate) =================")
deduped_df.explain(mode="formatted")

print("\n================= EXPLAIN: left join transactions vs customers =================")
joined_df.explain(mode="formatted")

print("\n================= EXPLAIN: top3 by province (window rank) =================")
top3_by_province_df.explain(mode="formatted")

spark.stop()
print("\nDONE.")