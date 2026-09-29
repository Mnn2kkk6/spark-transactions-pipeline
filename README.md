# Spark Transactions Pipeline (Luyện tập PySpark)

Bài luyện flow xử lý dữ liệu giao dịch bằng PySpark:

```
raw data → validate → deduplicate → join → window → aggregate → partitioned output
```

Ngoài pipeline chính, repo có 3 bài lab riêng đi sâu vào từng chủ đề:

| Bài | File | Nội dung |
|---|---|---|
| Pipeline | `pipeline.py` | validate, dedup, join, window, aggregate, ghi Parquet, `explain()` |
| Join | `join_lab.py` | inner join vs left join, vì sao ETL giữ lại record không map được |
| Partitioning | `partitioning_lab.py` | `repartition`, `coalesce`, `partitionBy`, ảnh hưởng tới số partition và số file output, small files |
| Performance | `performance_lab.py` | Shuffle, Broadcast Join, Cache/Persist, Data Skew (salting, AQE), đọc `explain()` (`Exchange`, `Sort`) |

## Cấu trúc repo

```
.
├── generate_data.py         # sinh dữ liệu mẫu (customers.csv, transactions.csv) kèm các case lỗi cố ý
├── pipeline.py               # pipeline PySpark đầy đủ (schema, validate, dedup, join, window, aggregate, write, verify, explain)
├── join_lab.py               # bài lab: inner join vs left join
├── partitioning_lab.py       # bài lab: repartition / coalesce / partitionBy, small files
├── performance_lab.py        # bài lab: shuffle, broadcast join, cache/persist, data skew, đọc explain()
├── data/
│   ├── customers.csv
│   └── transactions.csv
├── GIAI_THICH.md                      # lý thuyết pipeline (Window vs dropDuplicates, left vs inner join, bottleneck khi scale, phân tích explain())
├── PARTITIONING_LAB_GIAI_THICH.md     # giải thích partitioning lab (kết quả chạy thật + trả lời câu hỏi cuối bài)
├── PERFORMANCE_LAB_GIAI_THICH.md      # giải thích performance lab (số liệu đo thật, cách đọc plan, checklist tối ưu)
├── pipeline_run_log.txt               # log một lần chạy thật của pipeline.py
├── performance_lab_run_log.txt        # log một lần chạy thật của performance_lab.py
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

python3 join_lab.py               # bài lab join (dùng lại data/*.csv)
python3 partitioning_lab.py       # bài lab partitioning (dùng lại data/*.csv), output vào output_partitioning_lab/
python3 performance_lab.py        # bài lab performance, ~1.5 phút, tự sinh dữ liệu lớn trong Spark (không dùng data/*.csv)
python3 performance_lab.py 300000 # tuỳ chọn: giảm số transactions nếu máy yếu
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

## Performance lab (`performance_lab.py`)

Chạy trên dữ liệu tự sinh (1.000.000 transactions × 20.000 customers, có bản phân bố đều và bản lệch 60% vào 1 key) vì
`transactions.csv` chỉ ~106 dòng, quá nhỏ để đo được gì. Các bước:

1. **Shuffle**: đếm node `Exchange` trong plan để phân loại narrow / wide transformation (filter, groupBy, distinct, orderBy, Window, join...).
2. **Broadcast Join vs Sort-Merge Join**: so sánh plan, thời gian và lượng dữ liệu shuffle (21 MB so với 0 MB), kiểm tra 2 kiểu join cho cùng kết quả.
3. **Cache / Persist**: 3 action trên cùng 1 DataFrame tốn kém, có và không `cache()`; `unpersist()`.
4. **Data Skew**: phát hiện (phân bố key, số dòng mỗi partition), đo task chậm nhất qua Spark REST API, xử lý bằng salting (aggregate 2 giai đoạn + join), thử AQE skew join.
5. **Đọc `explain()`**: nhận biết `Exchange` / `Sort` / `BroadcastExchange`, đối chiếu "số stage = số Exchange + 1" bằng số liệu chạy thật.

Số đo thời gian chỉ có giá trị so sánh tương đối (chạy `local[*]`); chi tiết, giới hạn và checklist tối ưu nằm trong
[`PERFORMANCE_LAB_GIAI_THICH.md`](./PERFORMANCE_LAB_GIAI_THICH.md).

## Môi trường đã test

- Python 3.12
- PySpark 4.2.0 (chạy `local[*]`)
- OpenJDK 21

## License

MIT — dùng tự do cho mục đích học tập.
