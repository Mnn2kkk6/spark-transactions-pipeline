# Spark Transactions Pipeline (Luyện tập PySpark)

Bài luyện flow xử lý dữ liệu giao dịch bằng PySpark:

```
raw data → validate → deduplicate → join → window → aggregate → partitioned output
```

Repo gồm 3 phần:

1. **Pipeline chính** (`pipeline.py`) — flow đầy đủ từ raw data đến partitioned output.
2. **Join Lab** (`join_lab.py`) — bài tập riêng, đào sâu vào INNER JOIN vs LEFT JOIN, dùng lại đúng data của pipeline chính.
3. **Partitioning & Performance Lab** (`partitioning_lab.py`) — bài tập riêng về `repartition`/`coalesce`/`partitionBy`, cũng dùng lại đúng data của pipeline chính.

## Cấu trúc repo

```
.
├── generate_data.py           # sinh dữ liệu mẫu (customers.csv, transactions.csv) kèm các case lỗi cố ý
├── pipeline.py                 # pipeline PySpark đầy đủ (schema, validate, dedup, join, window, aggregate, write, verify, explain)
├── join_lab.py                 # bài tập riêng về JOIN: inner vs left, mapped/unmapped, so sánh kết quả
├── partitioning_lab.py         # bài tập riêng về Partitioning & Performance: repartition/coalesce/partitionBy
├── data/
│   ├── customers.csv
│   └── transactions.csv        # có thêm cột order_date, dùng chung cho cả 3 script
├── GIAI_THICH.md                # trả lời các câu hỏi lý thuyết của pipeline chính (Window vs dropDuplicates, left vs inner join, bottleneck khi scale, phân tích explain())
├── JOIN_LAB_GIAI_THICH.md       # trả lời các câu hỏi lý thuyết riêng của Join Lab (so sánh inner/left, vì sao ETL giữ lại unmapped)
├── PARTITIONING_LAB_GIAI_THICH.md  # trả lời các câu hỏi lý thuyết riêng của Partitioning Lab (repartition vs coalesce, partitionBy, small files)
├── pipeline_run_log.txt         # log của một lần chạy thật pipeline.py (số liệu, kết quả, physical plan)
├── join_lab_log.txt             # log của một lần chạy thật join_lab.py
├── partitioning_lab_run_log.txt # log của một lần chạy thật partitioning_lab.py
├── requirements.txt
└── .gitignore
```

## Dữ liệu

**customers.csv**: `customer_id, customer_name, province, created_at`

**transactions.csv**: `transaction_id, customer_id, amount, status, transaction_time, updated_at`

Dữ liệu mẫu (`generate_data.py`) cố tình cài các trường hợp:
- Cùng `transaction_id` xuất hiện nhiều lần với `updated_at` khác nhau (duplicate/update).
- Transaction thiếu `customer_id`.
- `amount <= 0`.
- `customer_id` không tồn tại trong `customers` (orphan).
- `status` gồm `SUCCESS`, `FAILED`, `PENDING`.
- Một khách hàng có nhiều transaction.

`join_lab.py` **dùng lại đúng 2 file CSV này** (coi `transactions.csv` là "orders") thay vì sinh data riêng, nên các case lỗi ở trên vẫn nguyên vẹn khi thực hành join.

`partitioning_lab.py` cũng dùng lại đúng 2 file này — LEFT JOIN `transactions` với `customers` để lấy thêm cột `province`, coi kết quả là "orders" cho bài Partitioning (order_id = transaction_id). Cột `order_date` vừa bổ sung ở trên chính là phục vụ bài này.

## Cách chạy

```bash
pip install -r requirements.txt   # cần Java 11/17/21 đã cài sẵn trên máy (Spark yêu cầu JVM)

python3 generate_data.py          # sinh data/customers.csv, data/transactions.csv (bắt buộc chạy trước, cả 3 script dưới đều cần data này)
python3 pipeline.py               # chạy pipeline chính, in log ra console
python3 join_lab.py               # chạy bài tập Join riêng, dùng lại đúng data vừa sinh
python3 partitioning_lab.py       # chạy bài tập Partitioning & Performance riêng, cũng dùng lại đúng data vừa sinh
```

> Trên Windows dùng lệnh `python` thay vì `python3`. Đường dẫn trong các script đều là đường dẫn
> **tương đối theo vị trí file** (`os.path.dirname(os.path.abspath(__file__))`), nên chạy được trên
> mọi hệ điều hành miễn là đứng đúng trong thư mục chứa script.

### Output của `pipeline.py`

Ghi vào thư mục `output/` (tự tạo khi chạy, không commit vào git — xem `.gitignore`):

- `output/valid_transactions_parquet/` — transaction hợp lệ (đã validate + dedup + map được customer), **partition theo `province`**.
- `output/error_records_parquet/` — record lỗi (thiếu `customer_id` hoặc `amount <= 0`), kèm cột `error_reason`.
- `output/unmapped_records_parquet/` — transaction có `customer_id` không match được với bảng `customers`.
- `output/customer_summary_parquet/` — tổng hợp theo customer: tổng số tx, tổng amount, số tx SUCCESS, transaction gần nhất.
- `output/top3_by_province_parquet/` — top 3 customer có tổng amount cao nhất theo từng province.

### Output của `join_lab.py`

Ghi vào thư mục `join_lab_output/` (cũng tự tạo, cũng bị bỏ qua trong git):

- `join_lab_output/mapped_orders_parquet/` — order join thành công với customer (kết quả left join, lọc `customer_name is not null`).
- `join_lab_output/unmapped_orders_parquet/` — order không map được customer (thiếu `customer_id` hoặc `customer_id` không tồn tại).

### Output của `partitioning_lab.py`

Ghi vào thư mục `output_partitioning_lab/` (tự tạo, bỏ qua trong git), gồm 1 thư mục Parquet riêng cho mỗi case thử nghiệm:

- `repartition_2_parquet/`, `repartition_4_parquet/`, `repartition_8_parquet/` — repartition theo số.
- `repartition_province_parquet/` — repartition theo cột `province` (hash partitioning, thường bị lệch).
- `coalesce_2_from_orig_parquet/` — coalesce(2) áp trực tiếp lên df gốc (chỉ 1 partition ban đầu nên không giảm được gì).
- `coalesce_2_from_8_parquet/` — coalesce(2) sau khi đã `repartition(8)`, minh hoạ coalesce giảm thật sự (8 → 2).
- `write_no_partitionby/` vs `write_with_partitionby/` — so sánh cấu trúc thư mục khi ghi có/không `partitionBy("province")`.
- `repartition_50_parquet/` — phần nâng cao, minh hoạ hiện tượng small files khi repartition quá nhiều so với dung lượng dữ liệu.

## Các bước xử lý chính (trong `pipeline.py`)

1. Đọc CSV bằng `StructType` khai báo sẵn — **không dùng `inferSchema`**.
2. Validate: tách record lỗi (thiếu `customer_id`, `amount <= 0`) ra `bad_records_df`.
3. Deduplicate: dùng `Window.partitionBy("transaction_id").orderBy("updated_at" desc)` + `row_number() == 1` để giữ bản ghi mới nhất cho mỗi `transaction_id`.
4. Left join `transactions` với `customers` theo `customer_id` → tách riêng `unmapped_df` (không map được customer).
5. Window: tìm transaction gần nhất của mỗi customer (`partitionBy("customer_id").orderBy("transaction_time" desc)`).
6. Aggregate theo customer: tổng số transaction, tổng amount, số transaction SUCCESS.
7. Window rank: top 3 customer theo tổng amount, tính riêng cho từng `province`.
8. Ghi `valid` ra Parquet, partition theo `province`; ghi `error`/`unmapped` ra output riêng.
9. Đọc lại các output vừa ghi và so sánh count với dữ liệu gốc để verify.
10. Dùng `explain(mode="formatted")` để xem physical plan, xác định chỗ có join / shuffle (`Exchange`) / sort.

Giải thích chi tiết từng câu hỏi lý thuyết (vì sao Window thay vì `dropDuplicates()`, vì sao left join, bước nào tốn tài nguyên nhất khi scale lên vài triệu record, và đọc `explain()`) nằm trong [`GIAI_THICH.md`](./GIAI_THICH.md).

## Bài tập Join riêng (`join_lab.py`)

Tách riêng khỏi flow chính để tập trung thuần vào khái niệm JOIN, join thẳng trên dữ liệu thô (không validate/dedup trước) để thấy rõ hành vi:

1. Đọc lại `data/customers.csv` và `data/transactions.csv` bằng schema khai báo sẵn (transactions đóng vai trò "orders").
2. **INNER JOIN** `orders` với `customers` — chỉ giữ order map được, order lỗi biến mất không dấu vết.
3. **LEFT JOIN** `orders` với `customers` — giữ toàn bộ order, order không map được có cột customer = `null`.
4. Tách `mapped_df` / `unmapped_df` từ kết quả left join.
5. So sánh số dòng `INNER JOIN` vs `LEFT JOIN`, chỉ ra chính xác những order nào bị inner join âm thầm loại bỏ.
6. `assert` kiểm tra bất biến: `mapped + unmapped = tổng orders gốc`.
7. Ghi `mapped`/`unmapped` ra Parquet riêng, `explain()` so sánh physical plan của inner join (`Join type: Inner`) và left join (`Join type: LeftOuter`).

Kết quả chạy thật (trên data 106 orders / 20 customers từ `generate_data.py`):

```
INNER JOIN: 97 dòng
LEFT JOIN : 106 dòng
Mapped: 97 | Unmapped: 9  (5 thiếu customer_id + 4 customer_id không tồn tại)
```

Giải thích đầy đủ — vì sao kết quả lệch nhau, và vì sao ETL thực tế nên giữ lại phần unmapped để kiểm tra/audit thay vì chỉ dùng inner join — nằm trong [`JOIN_LAB_GIAI_THICH.md`](./JOIN_LAB_GIAI_THICH.md).

## Bài tập Partitioning & Performance riêng (`partitioning_lab.py`)

Đọc lại `data/customers.csv` + `data/transactions.csv`, LEFT JOIN lấy thêm cột `province` rồi coi kết quả là "orders" (không sinh data riêng). Tắt Adaptive Query Execution (`spark.sql.adaptive.enabled=false`) để số partition hiển thị đúng lý thuyết thay vì bị AQE tự động gộp lại trên dataset nhỏ.

1. Đọc bằng schema khai báo sẵn, kiểm tra `df.rdd.getNumPartitions()` ban đầu.
2. Thử lần lượt `repartition(2)`, `repartition(4)`, `repartition(8)`, `repartition("province")`, `coalesce(2)` — mỗi case: kiểm tra số partition, số dòng/partition, `groupBy("province")` tính `total_orders`/`total_amount`/`avg_amount`, ghi Parquet, đếm số file `part-*` sinh ra.
3. So sánh `repartition` vs `coalesce`: khi nào tăng/giảm được partition, trường hợp nào gây shuffle, tạo bao nhiêu file.
4. So sánh ghi không `partitionBy` vs có `partitionBy("province")` — quan sát cấu trúc thư mục output khác nhau thế nào (bao gồm cả cách Spark xử lý giá trị `province = null`, gom vào `province=__HIVE_DEFAULT_PARTITION__/`).
5. Phần nâng cao: `repartition(50)` trên dataset nhỏ để thấy rõ hiện tượng small files.

Kết quả chạy thật (trên data 106 orders / 20 customers từ `generate_data.py`):

```
repartition(2/4/8): 2/4/8 partition, cân bằng đều
repartition("province"): 8 partition nhưng lệch (data skew theo hash)
coalesce(2) từ gốc (1 partition): vẫn là 1 — coalesce không thể tăng partition
repartition(8).coalesce(2): 2 partition — coalesce giảm thật sự
repartition(50): 50 file, mỗi file chỉ ~1.8 KB → minh hoạ small files problem
```

Trả lời đầy đủ 5 câu hỏi cuối bài (repartition khác coalesce thế nào, repartition theo cột khác theo số ra sao, partitionBy khi write khác repartition ở điểm nào, khi nào nên tăng/giảm số partition, vì sao không nên tạo quá nhiều file nhỏ) nằm trong [`PARTITIONING_LAB_GIAI_THICH.md`](./PARTITIONING_LAB_GIAI_THICH.md).

## Môi trường đã test

- Python 3.12
- PySpark 4.2.0 (chạy `local[*]`)
- OpenJDK 21

## License

MIT — dùng tự do cho mục đích học tập.
