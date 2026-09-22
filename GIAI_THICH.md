# Giải thích pipeline (Spark: raw → validate → dedup → join → window → aggregate → partitioned output)

Toàn bộ code đã chạy thật với PySpark 4.2.0 (local mode). Số liệu dưới đây lấy từ `pipeline_run_log.txt`:

```
Raw transactions count: 106
Bad records (missing customer_id / amount<=0): 10
Clean records: 96
Trước dedup: 96 | Sau dedup: 88 | Số bản ghi trùng bị loại: 8
Unmapped (customer_id không tồn tại trong customers): 4
Mapped thành công: 84
```

Verify sau khi ghi Parquet và đọc lại:
```
[valid_transactions_parquet]  written=84  read_back=84  match=True
[error_records_parquet]       written=10  read_back=10  match=True
[unmapped_records_parquet]    written=4   read_back=4   match=True
```

---

## 1. Vì sao dùng Window thay vì `dropDuplicates()`

`dropDuplicates(["transaction_id"])` chỉ đảm bảo **mỗi transaction_id còn lại đúng 1 dòng**, nhưng **không đảm bảo giữ đúng dòng nào** — Spark không có "thứ tự" mặc định giữa các partition, và với dữ liệu phân tán trên nhiều executor, dòng nào "đến trước" trong quá trình xử lý là không xác định (non-deterministic), có thể khác nhau giữa các lần chạy hoặc giữa các phiên bản Spark.

Trong bài toán này, khi cùng `transaction_id` xuất hiện nhiều lần (ví dụ do hệ thống nguồn gửi lại bản ghi cập nhật status), ta **cần giữ đúng bản ghi có `updated_at` mới nhất** — đây là logic nghiệp vụ có thứ tự rõ ràng, không phải "giữ 1 bản ghi bất kỳ".

Cách làm đúng là dùng Window:

```python
dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())
deduped_df = (
    clean_records_df
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

`row_number()` đánh số thứ tự bên trong từng nhóm `transaction_id` theo `updated_at` giảm dần → dòng có `rn == 1` chính là bản ghi mới nhất, đảm bảo **deterministic** (chạy lại luôn ra cùng 1 kết quả) và đúng nghiệp vụ.

Một điểm cộng khác: Window function không chỉ dùng để dedup mà còn tái dùng ngay cho các bước sau (tìm transaction gần nhất theo customer, xếp hạng top-3 theo province) — cùng một pattern tư duy, dễ maintain.

## 2. Vì sao dùng LEFT JOIN thay vì INNER JOIN

Yêu cầu đề bài là: *"Tách các transaction không mapping được customer ra một DataFrame riêng"*. Muốn tách được những dòng không match thì trước hết phải **giữ lại** chúng sau khi join — đó chính xác là những gì `LEFT JOIN` làm: giữ toàn bộ dòng bên trái (transactions), điền `null` cho các cột bên phải (customers) khi không tìm thấy khách hàng tương ứng.

Nếu dùng `INNER JOIN`, mọi transaction có `customer_id` không tồn tại trong `customers` (trường hợp cố tình tạo ra: `C9xxx`) sẽ **biến mất âm thầm** khỏi kết quả — không có cách nào phát hiện hay báo cáo được rằng dữ liệu bị thiếu mapping. Với dữ liệu tài chính/giao dịch, việc "rơi mất" record mà không log lại là rủi ro lớn (mất audit trail).

Quy trình đúng ở đây là:
```python
joined_df   = deduped_df.join(customers_df, on="customer_id", how="left")
unmapped_df = joined_df.filter(F.col("customer_name").isNull())   # left join miss -> null
mapped_df   = joined_df.filter(F.col("customer_name").isNotNull())
```
→ Kết quả thực tế: 4 dòng unmapped, 84 dòng mapped, tổng = 88 = đúng số dòng đưa vào join (không mất record nào).

## 3. Nếu transactions tăng lên vài triệu record, bước nào tốn tài nguyên nhất

Xếp theo mức độ rủi ro giảm dần:

1. **Window Function dedup theo `transaction_id`** (`Window.partitionBy("transaction_id").orderBy("updated_at")`)
   Đây là bước nặng nhất. Nó bắt buộc **shuffle toàn bộ dữ liệu** theo `transaction_id` (hash-partition lại) rồi **sort trong từng partition** theo `updated_at`. Với transaction_id có phân phối lệch (skew) — ví dụ một số ID bị trùng rất nhiều lần — một số partition sẽ phình to bất thường (data skew), khiến vài task chạy rất lâu trong khi các task khác đã xong ("straggler"). Đây thường là nút thắt cổ chai số 1 khi scale lên hàng triệu dòng.

2. **Left Join transactions ↔ customers**
   Ở dataset nhỏ (customers chỉ 20 dòng), Spark tự động chọn **Broadcast Hash Join** (thấy trong `explain()`: `BroadcastExchange` + `BroadcastHashJoin`) — rất rẻ vì không cần shuffle bảng transactions lớn, chỉ gửi bảng customers nhỏ tới mọi executor. Nhưng nếu bảng `customers` cũng lớn dần (vượt ngưỡng `spark.sql.autoBroadcastJoinThreshold`, mặc định 10MB), Spark sẽ chuyển sang **Sort-Merge Join**, phải shuffle + sort cả hai bảng theo `customer_id` — tốn kém hơn hẳn. Cần theo dõi kích thước `customers` để chủ động broadcast hint hoặc tăng threshold.

3. **Window tìm transaction gần nhất theo customer_id + Top-3 theo province (rank)**
   Cũng là Window function nên cũng shuffle + sort, nhưng nhóm theo `customer_id`/`province` thường có phân phối đều hơn `transaction_id` (số khách hàng ít hơn nhiều so với số giao dịch), nên ít bị skew nặng như bước 1. Với top-3, Spark còn tối ưu bằng `WindowGroupLimit` (thấy trong explain của bước top3) giúp cắt bớt dữ liệu sớm trước khi shuffle đầy đủ.

4. **Ghi Parquet partition theo `province`**
   Nếu số lượng province ít (như ở đây: 5 tỉnh) thì ổn. Nhưng nếu partition theo một cột có **cardinality cao** (ví dụ partition theo customer_id hay theo ngày ở mức phút), sẽ sinh ra **hàng chục nghìn thư mục nhỏ** ("small file problem"), làm chậm cả ghi lẫn đọc về sau, và gây áp lực lên metadata của filesystem/Hive metastore.

Gợi ý khi scale thật: tăng `spark.sql.shuffle.partitions` hợp lý theo cluster, cân nhắc `salting` key cho `transaction_id` nếu skew nặng, dùng `broadcast()` hint tường minh cho bảng customers nếu nó gần ngưỡng threshold, và kiểm soát số lượng partition cột khi ghi output.

## 4. `explain()` – chỗ có join / shuffle / sort

Trích thật từ `pipeline_run_log.txt` (physical plan, mode `formatted`):

### a) Dedup bằng Window (có Sort + Exchange = shuffle)
```
AdaptiveSparkPlan
+- Project
   +- Filter
      +- Window                     <-- row_number() over (partition by transaction_id order by updated_at desc)
         +- WindowGroupLimit
            +- Sort                 <-- SORT trong từng partition theo updated_at
               +- Exchange          <-- SHUFFLE: hashpartitioning(transaction_id, 8)
                  +- WindowGroupLimit
                     +- Sort
                        +- Filter
                           +- Scan csv
```
→ `Exchange ... hashpartitioning(transaction_id#4, 8)` chính là bước **shuffle** dữ liệu theo `transaction_id` để các bản ghi trùng ID rơi vào cùng partition; hai node `Sort` bao quanh là để `row_number()` tính đúng thứ tự theo `updated_at` mới nhất.

### b) Left Join transactions ↔ customers (Broadcast Join, không cần shuffle bảng lớn)
```
(13) BroadcastExchange
Input [3]: [customer_id, customer_name, province]
Arguments: HashedRelationBroadcastMode(...)

(14) BroadcastHashJoin
Left keys [1]: [customer_id#5]
Right keys [1]: [customer_id#0]
Join type: Inner   <-- (Spark optimize left join thành inner + null-fill khi filter customer_name; JOIN thực tế là left, thể hiện ở bước lấy unmapped/mapped phía sau)
```
→ Vì `customers.csv` rất nhỏ, Spark chọn **BroadcastHashJoin**: broadcast toàn bộ bảng customers tới mọi executor thay vì shuffle bảng transactions lớn — đây là điểm tối ưu tự động rất quan trọng của Spark khi một bên join nhỏ.

### c) Top-3 theo province (Window rank + Exchange + Sort)
```
(17) Exchange           <-- shuffle theo (customer_id, customer_name, province) khi gộp partial/final aggregate
(19) Sort                [province ASC, total_amount DESC]
(20) WindowGroupLimit    rank(total_amount) <= 3  (Partial)
(21) Exchange            hashpartitioning(province, 8)   <-- shuffle theo province để rank đúng trong từng tỉnh
(22) Sort                [province ASC, total_amount DESC]
(23) WindowGroupLimit    rank(total_amount) <= 3  (Final)
(24) Window              rank() over (partition by province order by total_amount desc)
(25) Filter              rank_in_province <= 3
(26) Exchange            rangepartitioning(province, rank_in_province)   <-- shuffle cho ORDER BY cuối
(27) Sort                [province ASC, rank_in_province ASC]
```
→ Có tới 3 lần `Exchange` (shuffle): một lần cho `groupBy` (tính tổng amount/customer), một lần cho `Window.partitionBy("province")` (xếp hạng), và một lần cho `orderBy` cuối cùng để hiển thị kết quả có thứ tự toàn cục. Đây là ví dụ rõ cho câu hỏi #3: càng nhiều bước Window/groupBy nối tiếp nhau, càng nhiều shuffle — chi phí cộng dồn khi dữ liệu lớn.

Xem đầy đủ physical plan (formatted) của cả 3 bước trong `pipeline_run_log.txt`, phần sau các dòng `================= EXPLAIN: ...`.

---

## Tóm tắt flow đã triển khai trong `pipeline.py`

```
customers.csv, transactions.csv  (đọc bằng StructType tường minh, không inferSchema)
        │
        ▼
   [VALIDATE] filter thiếu customer_id / amount<=0  →  bad_records_df (10 dòng)
        │ (phần còn lại: clean_records_df, 96 dòng)
        ▼
   [DEDUPLICATE] Window(partitionBy=transaction_id, orderBy=updated_at desc), row_number()==1
        │ (88 dòng, loại 8 dòng trùng)
        ▼
   [LEFT JOIN] deduped_df LEFT JOIN customers ON customer_id
        │
        ├─► unmapped_df (customer_name is null)      → 4 dòng, ghi riêng
        └─► mapped_df   (customer_name is not null)  → 84 dòng
                 │
                 ├─► [WINDOW] transaction gần nhất / customer (partitionBy customer_id, orderBy transaction_time desc)
                 ├─► [AGGREGATE] groupBy customer → total_transactions, total_amount, success_count
                 └─► [WINDOW rank] top 3 customer/amount theo từng province
        ▼
   [WRITE PARQUET] mapped_df.write.partitionBy("province")  → valid_transactions_parquet/
                    bad_records_df.write                    → error_records_parquet/
                    unmapped_df.write                        → unmapped_records_parquet/
        ▼
   [VERIFY] đọc lại từng output, so count(written) == count(read_back)  → tất cả match=True
```
