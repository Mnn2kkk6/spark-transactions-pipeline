# Performance Lab (PySpark): Shuffle, Broadcast Join, Cache/Persist, Data Skew, `explain()`

File thực hành: `performance_lab.py` — log một lần chạy thật: [`performance_lab_run_log.txt`](./performance_lab_run_log.txt).

```bash
python3 performance_lab.py            # mặc định 1.000.000 transactions, ~1.5 phút
python3 performance_lab.py 300000     # đổi số transactions nếu máy yếu
```

## Vì sao bài này không dùng `data/*.csv`?

`transactions.csv` chỉ có ~106 dòng. Với dữ liệu nhỏ như vậy mọi phép đo thời gian đều bị nhiễu
(JVM khởi động, JIT, overhead lập lịch còn lớn hơn thời gian xử lý thật), và không thể có data
skew đúng nghĩa. Nên bài này **sinh dữ liệu lớn hơn ngay trong Spark** (`spark.range`) với cùng
"hình dạng" như pipeline gốc: `customer_id`, `amount`, `status`, `province`.

- 1.000.000 transactions (8 partition đầu vào) × 20.000 customers.
- Bản `tx` phân bố **đều** theo `customer_id` (~50 dòng/customer).
- Bản `tx_skew`: **60% dòng dồn vào 1 customer** (`C00000`) để tạo data skew có kiểm soát.
- Không ghi/đè gì vào `data/`.

> **Cách đọc số liệu:** chạy `local[*]` trên máy 1 core, 3 GB RAM. Thời gian chỉ có giá trị
> **so sánh tương đối trong cùng lần chạy**, không phải benchmark. Các chỉ số không phụ thuộc phần
> cứng (số `Exchange`, MB shuffle, số dòng mỗi partition/task) đáng tin hơn thời gian. Chỗ nào
> số liệu nhiễu, doc nói rõ.

Script tắt AQE (`spark.sql.adaptive.enabled=false`) ở bước 1–4c để plan hiện đúng như lý thuyết
(AQE có thể tự đổi Sort-Merge Join thành Broadcast Join hoặc tự gộp partition, che mất điều cần
quan sát), rồi **bật lại ở bước 4d** để xem AQE tự xử lý skew.

---

## 1. Shuffle là gì, operation nào gây shuffle?

**Shuffle** = phân phối lại dữ liệu giữa các partition (và giữa các executor) để các dòng cùng
key nằm cùng chỗ. Nó ghi dữ liệu trung gian ra đĩa, truyền qua mạng, rồi đọc lại → **thường là
phần đắt nhất của một job Spark**. Trong physical plan, mỗi shuffle hiện thành 1 node `Exchange`.

- **Narrow transformation**: mỗi partition con chỉ phụ thuộc 1 partition cha (`filter`, `select`,
  `withColumn`, `coalesce`...) → không shuffle.
- **Wide transformation**: 1 partition con cần dữ liệu từ nhiều partition cha (`groupBy`, `join`,
  `distinct`, `orderBy`, Window, `repartition`...) → phải shuffle.

Kết quả đếm node trong plan (bước 1 của lab, đếm bằng code chứ không đếm tay):

| Case | Loại | `Exchange` | `BroadcastExchange` | `Sort` |
|---|---|:-:|:-:|:-:|
| `filter + select + withColumn` | narrow | 0 | 0 | 0 |
| `coalesce(2)` | narrow (chỉ gộp) | 0 | 0 | 0 |
| `repartition(8)` | wide | 1 | 0 | 0 |
| `groupBy(customer_id).agg(sum)` | wide | 1 | 0 | 0 |
| `distinct(customer_id)` | wide | 1 | 0 | 0 |
| `orderBy(amount)` | wide | 1 | 0 | 1 |
| Window `row_number()` by customer | wide | 1 | 0 | 1 |
| Join Sort-Merge | wide, shuffle **cả 2 bảng** | **2** | 0 | **2** |
| Join Broadcast | không shuffle bảng lớn | **0** | 1 | 0 |
| `repartition(cid)` rồi `groupBy(cid)` | shuffle 1 lần, dùng lại | 1 | 0 | 0 |

Các điểm đáng nhớ:

1. **`groupBy`/aggregation không shuffle "thô"**: Spark tách thành `HashAggregate (partial)` →
   `Exchange` → `HashAggregate (final)`. Bước partial gom sơ bộ ngay trong từng partition
   **trước khi** shuffle nên lượng dữ liệu phải chuyển giảm đi rất nhiều (map-side combine).
2. **Join thường**: Sort-Merge Join phải shuffle + sort cả hai bảng theo khoá join.
3. **Shuffle được dùng lại**: `repartition("customer_id")` rồi `groupBy("customer_id")` chỉ có
   **1** `Exchange`, vì dữ liệu đã được chia theo đúng key, `groupBy` không cần shuffle lại.
   Repartition sớm theo key chung có ích khi nhiều bước liên tiếp dùng cùng key.
4. Số partition sau shuffle = `spark.sql.shuffle.partitions` (mặc định **200**; lab và pipeline
   đặt 8 cho dữ liệu nhỏ). Quá nhiều → nhiều task nhỏ, overhead lập lịch; quá ít → partition
   quá to, dễ spill/OOM.

---

## 2. Broadcast Join vs Sort-Merge Join

Cùng join 1.000.000 tx với 20.000 customers, kết quả **giống hệt nhau** (lab có `assert` kiểm tra):

| | Sort-Merge Join | Broadcast Hash Join |
|---|---|---|
| Plan | 2 `Exchange` + 2 `Sort` + `SortMergeJoin` | `BroadcastExchange` + `BroadcastHashJoin` |
| Shuffle-write (Spark REST API) | **21,0 MB / 1.020.000 records** | **0 MB / 0 records** |
| Thời gian (best of 2) | 2,15 s | 1,25 s (nhanh hơn **~1,7×**) |

- Chỉ số shuffle (21 MB so với 0) là con số chắc chắn. Tỷ lệ thời gian dao động giữa các lần
  chạy (qua nhiều lần chạy thử em thấy khoảng 1,6×–2,1×); chênh lệch sẽ lớn hơn nhiều khi bảng lớn
  thật sự lớn và shuffle đi qua mạng thay vì local.
- Khi **không hint**, Spark vẫn tự chọn Broadcast vì `customers` (20.000 dòng) nhỏ hơn
  `spark.sql.autoBroadcastJoinThreshold` (10 MB): đó cũng là lý do `explain()` của
  `pipeline.py` (xem `GIAI_THICH.md` mục 4b) đã thấy `BroadcastHashJoin`.

**Khi nào dùng / không dùng:**

- **Dùng** khi một bên đủ nhỏ để nằm gọn trong bộ nhớ driver **và** mọi executor. Ép bằng
  `F.broadcast(df)` hoặc `df.hint("broadcast")`, hoặc chỉnh `autoBroadcastJoinThreshold`.
- **Không dùng** khi bảng "nhỏ" thực ra lớn: broadcast phải gom về driver rồi nhân bản ra mọi
  executor → tốn bộ nhớ, dễ OOM. Lúc đó Sort-Merge Join hợp lý hơn.
- Không broadcast được bên **được giữ lại** của outer join (left join thì không broadcast được bảng
  bên trái).
- Bonus: Broadcast Join **tránh được skew phía join** vì bảng lớn không bị shuffle theo key.
- Estimate kích thước của Spark có thể sai (đặc biệt sau nhiều bước biến đổi). Nếu biết chắc bảng
  nhỏ, dùng hint tường minh thay vì trông chờ vào ngưỡng tự động.

---

## 3. Cache / Persist

Mặc định Spark **không lưu kết quả trung gian**: mỗi action tính lại toàn bộ lineage từ đầu.
Bài lab dùng một DataFrame "tốn kém" (join Sort-Merge + groupBy, 2 lần shuffle) rồi chạy 3 action:
`count()`, tổng theo province, top 10 customer.

| Action | Không cache | Có cache |
|---|--:|--:|
| `count()` | 3,40 s | 2,58 s (**nạp cache**) |
| tổng theo province | 3,03 s | 0,46 s |
| top 10 customer | 2,85 s | 0,19 s |
| **Tổng** | **9,28 s** | **3,23 s (~2,9×)** |

Những điều bài lab chỉ ra:

- `cache()` là **lazy**: gọi xong mất ~0,08 s vì chưa tính gì; **action đầu tiên mới trả giá**
  (2,58 s, vẫn tính join + groupBy), các action sau đọc từ bộ nhớ.
- Khi đã cache, plan có `InMemoryTableScan` / `InMemoryRelation`; các node bên dưới
  `InMemoryRelation` chỉ là plan đã dùng để nạp cache, không chạy lại.
- `df.cache()` = `persist()` với level mặc định (`Disk Memory Deserialized`); `persist(StorageLevel.…)`
  cho phép chọn `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`...
- Nhớ `unpersist()` khi xong (lab kiểm tra `is_cached` trước/sau).

**Nên cache** khi DataFrame tốn kém và dùng lại từ 2 action trở lên. **Không nên** khi chỉ dùng
1 lần (thêm overhead, không lợi gì), khi dữ liệu quá lớn so với bộ nhớ (bị đẩy ra đĩa hoặc bị
evict), hoặc khi quên `unpersist()`. Cache chỉ sống trong 1 Spark application, không thay thế
việc ghi Parquet/checkpoint.

### Áp dụng vào chính `pipeline.py`

`pipeline.py` gọi `.count()` **17 lần**, trong đó 13 lần trên cùng vài DataFrame (`mapped_df`,
`unmapped_df`, `bad_records_df`, `deduped_df`). Mỗi lần đều **tính lại từ đầu** (đọc CSV →
filter → dedup Window có shuffle → left join). Ở 106 dòng thì không cảm nhận được, nhưng ở vài triệu
dòng đây là chỗ lãng phí điển hình: `mapped_df` là ứng viên rõ ràng để `cache()` (hoặc `persist`)
ngay sau bước join, rồi `unpersist()` ở cuối. Bài lab không sửa `pipeline.py`; đây là cải tiến
có thể làm tiếp.

---

## 4. Data Skew

**Data skew** = dữ liệu phân bố không đều theo key. Sau shuffle theo key, vài partition chứa quá
nhiều dòng. Một stage chỉ kết thúc khi **task chậm nhất (straggler)** xong, các core khác ngồi
chờ. Partition quá lớn còn dễ spill ra đĩa hoặc OOM.

### 4a. Phát hiện

Đếm số dòng theo key và số dòng mỗi partition sau khi chia theo key:

```
Top customer_id (tx_skew, 1.000.000 dòng):  C00000: 599.983 (60,0%) | các key khác: ~35 dòng
Số dòng mỗi partition khi shuffle theo customer_id:
  tx (đều) : max 128.050 dòng (13%)  | lớn nhất / median = 1,0x
  tx_skew  : max 650.021 dòng (65%)  | lớn nhất / median = 12,9x
```

### 4b. Skew làm task chậm bất thường (đo từng task qua Spark REST API)

Stage reduce của Sort-Merge Join theo `customer_id`:

| | Dòng shuffle-read: median → max | Thời gian task: median → max |
|---|---|---|
| Dữ liệu **đều** | 128.367 → 130.611 (×1,0) | 116 ms → 198 ms (×1,7) |
| Dữ liệu **lệch** | 52.874 → **652.528 (×12,3)** | 56 ms → **627 ms (×11,2)** |

Một task phải đọc gấp ~12 lần dữ liệu và chạy chậm gấp ~11 lần task trung vị. Trên cluster nhiều
core, cả stage phải chờ task này.

### 4c. Xử lý bằng salting

Ý tưởng: thêm cột `salt` ngẫu nhiên (0…S−1) để **chia hot key thành S mảnh**, mỗi mảnh vào một
partition khác nhau.

- **Aggregate** (2 giai đoạn): `groupBy(key, salt)` để gom từng mảnh → `groupBy(key)` để gộp lại.
  Kết quả được kiểm tra **khớp hoàn toàn** với `groupBy(key)` trực tiếp trên cả 20.000 customer.
- **Join**: thêm `salt` ngẫu nhiên vào bảng lớn, **nhân bản bảng nhỏ S lần** (mỗi dòng ứng với
  mọi giá trị salt), join theo `(key, salt)`. Kết quả cũng được assert khớp.

Kết quả đo (S = 8):

| | Partition/task lớn nhất | Ghi chú |
|---|---|---|
| Chưa salt | 650.021 dòng (65%) | ×12,9 so với median |
| Salt 8 bucket, chia theo (key, salt) | 275.269 dòng (28%) | ×2,2 so với median |
| Task join: dòng shuffle-read max | 652.528 → **295.277** | giảm ~2,2× |

Ảnh hưởng của số salt bucket tới độ đều (shuffle 8 partition):

| Số salt bucket | Partition lớn nhất chiếm | Chi phí |
|---|---|---|
| 8 | 28% | bảng nhỏ nhân bản ×8 |
| 16 | 24% | ×16 |
| 32 | 20% | ×32 |

Salting **không bao giờ cho phân bố đều tuyệt đối** vì hash của `(key, salt)` có thể đụng nhau ở
mức partition. Nhiều salt hơn thì đều hơn, nhưng đổi lại bảng nhỏ phình to.

> **Thẳng thắn về giới hạn của số đo thời gian:** thời gian tuyệt đối của từng task dao động khá
> nhiều giữa các lần chạy trên máy 1 core (ở các lần chạy thử, task chậm nhất của bản lệch dao
> động 410–707 ms; bản đã salt 318–421 ms). Có lần salting gần như không giảm được thời gian task
> tối đa. Tổng thời gian job join trên local thậm chí **tăng** (2,00 s → 2,71 s trong lần chạy cuối) vì phải
> nhân bản bảng nhỏ. Chỉ số ổn định qua mọi lần chạy là **số dòng task lớn nhất phải xử lý** (652 k → 295 k).
> Lợi ích thật của salting là trên cluster nhiều core, nơi thời gian stage ≈ thời gian task chậm
> nhất. Local 1 core không đủ để chứng minh điều đó bằng đồng hồ.

### 4d. AQE skew join (Spark tự xử lý)

Bật lại AQE, hạ ngưỡng skew xuống cho vừa dataset bài lab (mặc định là partition > 256 MB và > 5×
median, quá lớn với 1 triệu dòng):

```
spark.sql.adaptive.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 2
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 1MB
```

Plan cuối cùng có `SortMergeJoin(skew=true)` và `AQEShuffleRead ... skewed`: AQE phát hiện partition
lệch lúc runtime và **tự chia nó thành nhiều task**. Task lớn nhất giảm từ 652.528 xuống
**165.094 dòng** (giảm ~4×) mà không cần sửa code nghiệp vụ.
Lưu ý: AQE chỉ can thiệp khi partition vượt ngưỡng. Khi em chạy lab với 300.000 dòng
(`python3 performance_lab.py 300000`), với cùng ngưỡng này AQE **không** kích hoạt skew join
(script in `skew=true: False`); cần dữ liệu đủ lớn hoặc hạ ngưỡng thêm.
Trên production nên **để AQE bật** và chỉ chỉnh ngưỡng nếu cần; salting thủ công dành cho trường
hợp AQE không xử lý được (ví dụ skew ở `groupBy` thay vì join, hoặc key skew quá nặng).

**Các cách khác** (không chạy trong lab): broadcast bảng nhỏ (hết skew phía join); tách hot key ra
xử lý riêng rồi `union` lại; lọc key rác (vd `customer_id` null/rỗng dồn hết vào 1 partition) trước
khi join. Ví dụ ngay trong dữ liệu của repo này: 5 giao dịch thiếu `customer_id` đều rơi vào một
key rỗng, và ở `partitionBy("province")` các dòng `province = null` gom vào
`__HIVE_DEFAULT_PARTITION__`. Với dữ liệu lớn, key null như vậy là nguồn skew rất phổ biến.

---

## 5. Đọc `explain()`

Dùng `df.explain(mode="formatted")`: phần trên là **cây plan**, phần dưới là chi tiết từng node
đánh số `(1)`, `(2)`... Cách đọc:

- **Đọc từ dưới lên trên** = thứ tự thực thi; nhánh thụt vào nhiều hơn chạy trước.
- Dấu `*` ở đầu node = node đó nằm trong whole-stage codegen.
- `Exchange` = **shuffle = ranh giới stage**. Không có Exchange thì các node cùng nằm 1 stage.

Query ví dụ trong lab (join bảng nhỏ → filter → groupBy → orderBy):

```
* Sort (13)
+- Exchange (12)                         <- shuffle #2: rangepartitioning cho ORDER BY toàn cục
   +- * HashAggregate (11)               <- aggregate final
      +- Exchange (10)                   <- shuffle #1: hashpartitioning(province)
         +- * HashAggregate (9)          <- aggregate partial (trước shuffle)
            +- * Project (8)
               +- * BroadcastHashJoin Inner BuildRight (7)
                  :- * Filter (3)
                  :  +- * Project (2)
                  :     +- * Range (1)
                  +- BroadcastExchange (6)   <- gửi bảng nhỏ tới mọi executor, KHÔNG phải shuffle bảng lớn
                     +- * Project (5)
                        +- * Range (4)
```

| Node | Ý nghĩa | Chi phí |
|---|---|---|
| `Filter`, `Project` | narrow, xử lý từng dòng | rẻ |
| `HashAggregate` (2 lần) | partial (trước shuffle) + final (sau shuffle) | vừa |
| `Exchange hashpartitioning(k, N)` | **shuffle** theo hash của key `k` thành `N` partition | **đắt** |
| `Exchange rangepartitioning` + `Sort` | shuffle cho `ORDER BY` toàn cục | **đắt** |
| `BroadcastExchange` | gửi bảng nhỏ tới mọi executor | rẻ nếu bảng thật sự nhỏ |
| `Sort` | sắp xếp trong partition (Sort-Merge Join, Window, ORDER BY) | vừa–đắt |
| `SortMergeJoin` | join 2 bảng đã shuffle + sort | đắt |
| `InMemoryTableScan` | đọc từ cache | rẻ |

**Kiểm chứng "số stage = số Exchange + 1"**: plan trên có 2 `Exchange` → job chính có **3 stage**.
Lab chạy query thật và đếm stage qua `statusTracker`: một action `collect()` sinh ra **3 job** có
1, 2 và 3 stage. Job 3 stage khớp đúng dự đoán (2 `Exchange` + 1) và lab có `assert` cho điều này.
Hai job nhỏ còn lại nhiều khả năng là job build `BroadcastExchange` và job lấy mẫu để chia range cho
`orderBy` (đây là suy luận từ số stage, lab không kiểm chứng trực tiếp; muốn chắc thì xem tab Jobs
trong Spark UI). Điểm cần nhớ: một action có thể sinh nhiều job, nên tổng số stage lớn hơn
số `Exchange` + 1.

**Checklist khi đọc plan để tối ưu:**

1. Đếm `Exchange`: mỗi cái là một lần shuffle; có bỏ bớt / dùng lại được không (repartition sớm
   theo key chung, broadcast bảng nhỏ, gộp bước `groupBy`)?
2. Thấy `SortMergeJoin` + 2 `Exchange` + 2 `Sort` mà một bên nhỏ → cân nhắc `F.broadcast()`.
3. `Exchange hashpartitioning(key, N)`: key có bị skew không? `N` (`shuffle.partitions`) có hợp lý không?
4. `Exchange rangepartitioning` + `Sort` ở cuối là `ORDER BY` toàn cục: nếu không cần thứ tự toàn cục
   thì bỏ đi hoặc dùng `sortWithinPartitions`.
5. Đã `cache()` mà plan không có `InMemoryTableScan` → cache chưa được dùng (chưa có action nào nạp,
   hoặc plan dùng nhánh khác).
6. Plan chỉ là **kế hoạch**; muốn biết task nào chậm, bao nhiêu MB shuffle, có spill không thì
   xem **Spark UI** (tab Stages / SQL) hoặc REST API như lab đã làm.

Các plan thật của pipeline (dedup Window, left join, top-3 theo province) đã được phân tích trong
[`GIAI_THICH.md`](./GIAI_THICH.md) mục 4; bài lab này bổ sung phần đo đạc và so sánh.

---

## Tổng kết: job chạy chậm thì kiểm tra theo thứ tự nào?

1. `explain()` → đếm `Exchange`, tìm `SortMergeJoin` không cần thiết → dùng `broadcast` nếu bảng nhỏ.
2. Xem Spark UI: có task nào lâu / đọc nhiều dữ liệu hơn hẳn median không? → nghi **skew** → kiểm tra
   phân bố key, thử AQE skew join, rồi salting nếu cần.
3. Có DataFrame nào được dùng lại nhiều action mà tính lại từ đầu không? → `cache()`/`persist()`,
   nhớ `unpersist()`.
4. Số partition sau shuffle có hợp lý không (`spark.sql.shuffle.partitions`, AQE coalesce)? Và khi
   ghi: số file có quá nhiều/quá ít không (xem `PARTITIONING_LAB_GIAI_THICH.md`).
