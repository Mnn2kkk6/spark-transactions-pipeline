# Spark Transactions Pipeline (Luyện tập PySpark)

Bài luyện flow xử lý dữ liệu giao dịch bằng PySpark:

```
raw data → validate → deduplicate → join → window → aggregate → partitioned output
```

## Cấu trúc repo

```
.
├── generate_data.py         # sinh dữ liệu mẫu (customers.csv, transactions.csv) kèm các case lỗi cố ý
├── pipeline.py               # pipeline PySpark đầy đủ (schema, validate, dedup, join, window, aggregate, write, verify, explain)
├── data/
│   ├── customers.csv
│   └── transactions.csv
├── GIAI_THICH.md              # trả lời các câu hỏi lý thuyết (Window vs dropDuplicates, left vs inner join, bottleneck khi scale, phân tích explain())
├── pipeline_run_log.txt       # log của một lần chạy thật (số liệu, kết quả, physical plan)
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

## Cách chạy

```bash
pip install -r requirements.txt   # cần Java 11/17/21 đã cài sẵn trên máy (Spark yêu cầu JVM)

python3 generate_data.py          # sinh lại data/customers.csv, data/transactions.csv
python3 pipeline.py               # chạy toàn bộ pipeline, in log ra console
```

Output được ghi vào thư mục `output/` (tự tạo khi chạy, không commit vào git — xem `.gitignore`):

- `output/valid_transactions_parquet/` — transaction hợp lệ (đã validate + dedup + map được customer), **partition theo `province`**.
- `output/error_records_parquet/` — record lỗi (thiếu `customer_id` hoặc `amount <= 0`), kèm cột `error_reason`.
- `output/unmapped_records_parquet/` — transaction có `customer_id` không match được với bảng `customers`.
- `output/customer_summary_parquet/` — tổng hợp theo customer: tổng số tx, tổng amount, số tx SUCCESS, transaction gần nhất.
- `output/top3_by_province_parquet/` — top 3 customer có tổng amount cao nhất theo từng province.

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

## Môi trường đã test

- Python 3.12
- PySpark 4.2.0 (chạy `local[*]`)
- OpenJDK 21

## License

MIT — dùng tự do cho mục đích học tập.
