# Spark Transactions Pipeline (Luyện tập PySpark)

Bài luyện flow xử lý dữ liệu giao dịch bằng PySpark:

```
raw data → validate → deduplicate → join → window → aggregate → partitioned output
```

Repo gồm 2 phần:

1. **Pipeline chính** (`pipeline.py`) — flow đầy đủ từ raw data đến partitioned output.
2. **Join Lab** (`join_lab.py`) — bài tập riêng, đào sâu vào INNER JOIN vs LEFT JOIN, dùng lại đúng data của pipeline chính.

## Cấu trúc repo

```
.
├── generate_data.py           # sinh dữ liệu mẫu (customers.csv, transactions.csv) kèm các case lỗi cố ý
├── pipeline.py                 # pipeline PySpark đầy đủ (schema, validate, dedup, join, window, aggregate, write, verify, explain)
├── join_lab.py                 # bài tập riêng về JOIN: inner vs left, mapped/unmapped, so sánh kết quả
├── data/
│   ├── customers.csv
│   └── transactions.csv
├── GIAI_THICH.md                # trả lời các câu hỏi lý thuyết của pipeline chính (Window vs dropDuplicates, left vs inner join, bottleneck khi scale, phân tích explain())
├── JOIN_LAB_GIAI_THICH.md       # trả lời các câu hỏi lý thuyết riêng của Join Lab (so sánh inner/left, vì sao ETL giữ lại unmapped)
├── pipeline_run_log.txt         # log của một lần chạy thật pipeline.py (số liệu, kết quả, physical plan)
├── join_lab_log.txt             # log của một lần chạy thật join_lab.py
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

## Cách chạy

```bash
pip install -r requirements.txt   # cần Java 11/17/21 đã cài sẵn trên máy (Spark yêu cầu JVM)

python3 generate_data.py          # sinh data/customers.csv, data/transactions.csv (bắt buộc chạy trước, cả 2 script dưới đều cần data này)
python3 pipeline.py               # chạy pipeline chính, in log ra console
python3 join_lab.py               # chạy bài tập Join riêng, dùng lại đúng data vừa sinh
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

## Môi trường đã test

- Python 3.12
- PySpark 4.2.0 (chạy `local[*]`)
- OpenJDK 21

## License

MIT — dùng tự do cho mục đích học tập.
