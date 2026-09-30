# Spark Transactions Pipeline (Luyện tập PySpark)

Repo luyện tập flow xử lý dữ liệu giao dịch bằng **PySpark**, từ Batch ETL đến **Structured Streaming với Kafka**.

```text
Batch:
raw data → validate → deduplicate → join → window → aggregate → partitioned output

Streaming:
transactions.csv → Kafka Producer → Kafka topic → Spark Structured Streaming
                                              ↓
                              JSON → schema → validate → output
                                              ↓
                                  console / Parquet / aggregate
```

Ngoài pipeline chính, repo có các bài lab đi sâu vào **Join, Partitioning, Performance và Structured Streaming**.

## Các bài lab

| Bài                    | File                                   | Nội dung                                                                            |
| ---------------------- | -------------------------------------- | ----------------------------------------------------------------------------------- |
| Pipeline               | `pipeline.py`                          | validate, dedup, join, window, aggregate, ghi Parquet, `explain()`                  |
| Join                   | `join_lab.py`                          | inner join vs left join, vì sao ETL giữ lại record không map được                   |
| Partitioning           | `partitioning_lab.py`                  | `repartition`, `coalesce`, `partitionBy`, số partition, số file output, small files |
| Performance            | `performance_lab.py`                   | Shuffle, Broadcast Join, Cache/Persist, Data Skew, salting, AQE, đọc `explain()`    |
| Streaming Transactions | `streaming/streaming_transactions.py`  | đọc Kafka bằng `readStream`, parse JSON, schema, validation, console/Parquet        |
| Streaming Aggregate    | `streaming/streaming_aggregate_lab.py` | aggregation theo `status`, thử `update` và `complete` mode                          |
| Kafka Producer         | `streaming/kafka_producer.py`          | đọc `transactions.csv` và gửi transaction vào Kafka                                 |

---

## Cấu trúc repo

```text
.
├── generate_data.py
├── pipeline.py
├── join_lab.py
├── partitioning_lab.py
├── performance_lab.py
│
├── streaming/
│   ├── kafka_producer.py
│   ├── streaming_transactions.py
│   └── streaming_aggregate_lab.py
│
├── data/
│   ├── customers.csv
│   └── transactions.csv
│
├── output/
│   └── ...
│
├── output_streaming/
│   └── ...
│
├── GIAI_THICH.md
├── PARTITIONING_LAB_GIAI_THICH.md
├── PERFORMANCE_LAB_GIAI_THICH.md
├── pipeline_run_log.txt
├── performance_lab_run_log.txt
├── requirements.txt
└── .gitignore
```

Output runtime như `output/`, `output_streaming/` và các thư mục checkpoint không cần commit vào Git.

---

# 1. Dữ liệu

## `customers.csv`

```text
customer_id,customer_name,province,created_at
```

## `transactions.csv`

```text
transaction_id,customer_id,amount,status,transaction_time,updated_at
```

Dữ liệu mẫu được sinh bởi `generate_data.py` và cố tình chứa nhiều trường hợp để luyện tập:

* Cùng `transaction_id` xuất hiện nhiều lần với `updated_at` khác nhau.
* Transaction thiếu `customer_id`.
* `amount <= 0`.
* `customer_id` không tồn tại trong `customers`.
* `status` gồm `SUCCESS`, `FAILED`, `PENDING`.
* Một customer có nhiều transaction.

---

# 2. Batch Pipeline

Pipeline chính trong `pipeline.py`:

```text
CSV
 ↓
Read with explicit schema
 ↓
Validate
 ├── valid
 └── error
 ↓
Deduplicate
 ↓
Left Join customers
 ├── mapped
 └── unmapped
 ↓
Window
 ↓
Aggregate by customer
 ↓
Top 3 by province
 ↓
Write Parquet
 ↓
Verify + explain()
```

## Các bước xử lý chính

1. Đọc CSV bằng `StructType` khai báo sẵn, không dùng `inferSchema`.

2. Validate:

   * thiếu `customer_id`
   * `amount <= 0`

   Record lỗi được tách ra `bad_records_df`.

3. Deduplicate:

```python
Window.partitionBy("transaction_id") \
      .orderBy(col("updated_at").desc())
```

Giữ bản ghi mới nhất bằng `row_number() == 1`.

4. Left join transactions với customers theo `customer_id`.

5. Tách transaction không map được customer vào `unmapped_df`.

6. Sử dụng Window để tìm transaction gần nhất của từng customer.

7. Aggregate theo customer:

   * tổng số transaction
   * tổng amount
   * số transaction `SUCCESS`

8. Tính top 3 customer có tổng amount cao nhất theo từng `province`.

9. Ghi kết quả ra Parquet.

10. Đọc lại output để verify count và dùng:

```python
explain(mode="formatted")
```

để quan sát physical plan, đặc biệt là `Exchange`, `Sort`, join và shuffle.

---

# 3. Batch Output

Sau khi chạy `pipeline.py`, các output chính:

```text
output/
├── valid_transactions_parquet/
├── error_records_parquet/
├── unmapped_records_parquet/
├── customer_summary_parquet/
└── top3_by_province_parquet/
```

Trong đó:

### `valid_transactions_parquet/`

Transaction hợp lệ, đã:

* validate
* deduplicate
* map được customer

Output được partition theo:

```text
province
```

### `error_records_parquet/`

Các record lỗi do:

```text
thiếu customer_id
amount <= 0
```

kèm theo:

```text
error_reason
```

### `unmapped_records_parquet/`

Transaction có `customer_id` nhưng không tìm thấy customer tương ứng.

### `customer_summary_parquet/`

Tổng hợp theo customer:

```text
total_transactions
total_amount
success_transactions
latest_transaction
```

### `top3_by_province_parquet/`

Top 3 customer có tổng amount cao nhất trong từng province.

---

# 4. Performance Lab

`performance_lab.py` sử dụng dữ liệu lớn được sinh trực tiếp trong Spark vì `transactions.csv` chỉ khoảng 106 dòng, không đủ lớn để đo hiệu năng.

Lab sử dụng:

```text
1,000,000 transactions
20,000 customers
```

và có cả dữ liệu phân bố lệch với khoảng 60% record tập trung vào một key.

## Nội dung

### Shuffle

Quan sát `Exchange` trong physical plan để phân biệt:

```text
Narrow Transformation
    ↓
Filter
Map
...

Wide Transformation
    ↓
GroupBy
Distinct
OrderBy
Window
Join
...
```

### Broadcast Join vs Sort-Merge Join

So sánh:

* physical plan
* thời gian chạy
* dữ liệu shuffle

Ví dụ số liệu thực nghiệm trong lab:

```text
Sort-Merge Join: khoảng 21 MB shuffle
Broadcast Join: 0 MB shuffle
```

### Cache / Persist

So sánh nhiều action trên cùng DataFrame:

```python
df.cache()
```

và không cache, sau đó:

```python
df.unpersist()
```

### Data Skew

Phân tích:

* phân bố key
* số dòng trên mỗi partition
* task chậm nhất

Thử các hướng xử lý:

```text
Salting
AQE Skew Join
```

### Đọc `explain()`

Tập trung nhận biết:

```text
Exchange
Sort
BroadcastExchange
```

và đối chiếu số stage với số `Exchange`.

Chi tiết nằm trong:

[`PERFORMANCE_LAB_GIAI_THICH.md`](./PERFORMANCE_LAB_GIAI_THICH.md)

---

# 5. Structured Streaming + Kafka

Phần Streaming mở rộng `pipeline.py` từ xử lý **Batch** sang xử lý dữ liệu liên tục bằng **Spark Structured Streaming**.

## Kiến trúc

```text
transactions.csv
      |
      v
kafka_producer.py
      |
      v
Kafka topic: transactions
      |
      v
Spark Structured Streaming
      |
      +--> readStream(kafka)
      |
      +--> JSON -> schema
      |
      +--> validate
      |
      +--> output
              |
              +--> append -> console / Parquet
              |
              +--> update / complete
                       |
                       +--> console aggregation
```

Kafka được dùng làm nguồn dữ liệu streaming, còn Spark chịu trách nhiệm đọc, parse, validate và xử lý dữ liệu.

Spark Kafka integration sử dụng:

```text
spark-sql-kafka-0-10_2.13
```

Các lệnh trong lab đang pin connector:

```text
4.0.0
```

và repo yêu cầu môi trường:

```text
pyspark >= 4.0.0
```

---

# 6. Chuẩn bị Kafka

Lab giả định Kafka broker đang listen tại:

```text
localhost:9092
```

Tạo topic `transactions`:

```bash
kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1
```

Kiểm tra topic:

```bash
kafka-topics --bootstrap-server localhost:9092 \
  --describe \
  --topic transactions
```

Topic được tạo với:

```text
3 partitions
1 replication factor
```

---

# 7. Cài Kafka Producer

Cài thư viện:

```bash
pip install kafka-python-ng
```

Producer nằm tại:

```text
streaming/kafka_producer.py
```

Producer đọc dữ liệu từ:

```text
data/transactions.csv
```

sau đó chuyển mỗi transaction thành JSON và gửi vào Kafka topic.

`transaction_id` được sử dụng làm Kafka key để có thể quan sát cách record được phân phối vào các partition.

---

# 8. Chạy Kafka Producer

Từ thư mục repo:

```bash
python streaming/kafka_producer.py \
  --topic transactions \
  --limit 50 \
  --delay 0.5
```

Trong đó:

```text
--topic     Kafka topic
--limit     số transaction tối đa được gửi
--delay     thời gian chờ giữa các message
```

Ví dụ:

```text
transactions.csv
      |
      +--> tx_001
      +--> tx_002
      +--> tx_003
      ...
```

sẽ trở thành các Kafka message:

```json
{
  "transaction_id": "tx_001",
  "customer_id": "cus_001",
  "amount": 150000,
  "status": "SUCCESS",
  "transaction_time": "...",
  "updated_at": "..."
}
```

---

# 9. Structured Streaming — Append Mode

Chạy:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/streaming_transactions.py \
  --brokers localhost:9092 \
  --topic transactions \
  --starting-offsets earliest \
  --sink console
```

Spark sẽ:

```text
Kafka
 ↓
value
 ↓
JSON parsing
 ↓
Schema
 ↓
Validation
 ↓
Valid transactions
 ↓
Console
```

Schema được xây dựng dựa trên cấu trúc của `transactions.csv`.

Các field Kafka metadata được giữ lại:

```text
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
```

Ví dụ:

```text
transaction_id = tx_001
kafka_partition = 1
kafka_offset = 42
```

## Ghi ra Parquet

Thay vì console:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/streaming_transactions.py \
  --brokers localhost:9092 \
  --topic transactions \
  --sink parquet \
  --output output_streaming/valid_transactions
```

Output được ghi vào:

```text
output_streaming/valid_transactions/
```

---

# 10. Validation trong Streaming

Streaming cũng thực hiện validation tương tự Batch.

Các record lỗi gồm:

```text
customer_id is null
amount <= 0
```

Mục đích là kiểm tra xem dữ liệu phát sinh liên tục từ Kafka có đáp ứng các điều kiện đầu vào hay không.

Có thể chủ động gửi transaction lỗi để kiểm tra:

```text
amount <= 0
```

hoặc:

```text
customer_id bị thiếu
```

---

# 11. Update Mode

Chạy aggregation lab:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/streaming_aggregate_lab.py \
  --brokers localhost:9092 \
  --topic transactions \
  --mode update
```

Sau đó chạy producer để gửi thêm dữ liệu.

Ở `update` mode, console hiển thị những nhóm aggregate vừa được thay đổi trong trigger.

Ví dụ aggregate theo:

```text
status
```

có thể có:

```text
SUCCESS
FAILED
PENDING
```

Khi một transaction mới có:

```text
status = SUCCESS
```

giá trị aggregate của `SUCCESS` được cập nhật.

---

# 12. Complete Mode

Chạy:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/streaming_aggregate_lab.py \
  --brokers localhost:9092 \
  --topic transactions \
  --mode complete
```

Ở `complete` mode, mỗi trigger sẽ in **toàn bộ bảng aggregate hiện tại**.

Ví dụ:

```text
SUCCESS  35
FAILED   10
PENDING   5
```

Khi có thêm transaction:

```text
SUCCESS  36
FAILED   10
PENDING   5
```

toàn bộ bảng aggregate được xuất lại.

---

# 13. Checkpoint

Mỗi streaming query có checkpoint riêng:

```text
output_streaming/
├── checkpoint_transactions/
├── checkpoint_aggregate_update/
└── checkpoint_aggregate_complete/
```

Checkpoint được sử dụng để Spark lưu tiến độ xử lý và state cần thiết để khôi phục query.

## Thử nghiệm recovery

### Bước 1

Start Spark Structured Streaming.

### Bước 2

Start Kafka Producer và gửi một số message.

### Bước 3

Dừng Spark:

```text
Ctrl + C
```

### Bước 4

Start lại query với **cùng checkpoint**.

### Bước 5

Quan sát Spark tiếp tục xử lý dựa trên tiến độ đã được lưu thay vì coi đây là một query hoàn toàn mới.

Không dùng chung một checkpoint cho hai query có logic khác nhau.

Ví dụ:

```text
checkpoint_transactions/
```

không nên dùng chung cho:

```text
streaming_aggregate_lab.py
```

vì đây là hai query có logic và state khác nhau.

---

# 14. Kafka Topic / Partition / Offset

Topic `transactions` được tạo với 3 partition:

```text
Topic: transactions
       |
       +-- partition 0 -> offset 0, 1, 2, 3, ...
       |
       +-- partition 1 -> offset 0, 1, 2, 3, ...
       |
       +-- partition 2 -> offset 0, 1, 2, 3, ...
```

Có thể hiểu:

* **Topic**: nơi chứa nhóm message cùng loại.
* **Partition**: các vùng lưu trữ độc lập bên trong topic.
* **Offset**: vị trí của một record trong từng partition.

Ví dụ:

```text
transaction_id = tx_001
kafka_partition = 1
kafka_offset = 42
```

nghĩa là record `tx_001` đang nằm ở:

```text
topic: transactions
partition: 1
offset: 42
```

Structured Streaming sử dụng Kafka offset để xác định phạm vi dữ liệu cần đọc trong từng micro-batch và lưu tiến độ vào checkpoint.

---

# 15. `startingOffsets`: earliest vs latest

Có thể thay đổi:

```bash
--starting-offsets earliest
```

thành:

```bash
--starting-offsets latest
```

## `earliest`

Spark bắt đầu đọc từ các offset cũ nhất còn có thể đọc trong Kafka.

Phù hợp khi muốn xử lý lại dữ liệu đã tồn tại trong topic.

## `latest`

Spark bắt đầu từ dữ liệu mới được ghi vào Kafka sau thời điểm query bắt đầu.

Phù hợp với trường hợp muốn theo dõi dữ liệu mới phát sinh.

Lưu ý: `startingOffsets` chủ yếu có ý nghĩa khi query được khởi tạo với một checkpoint mới. Khi query đã có checkpoint, Spark sẽ sử dụng tiến độ đã lưu trong checkpoint.

---

# 16. Batch vs Structured Streaming

| Batch (`pipeline.py`)          | Structured Streaming                     |
| ------------------------------ | ---------------------------------------- |
| `spark.read.csv(...)`          | `spark.readStream.format("kafka")`       |
| Dataset hữu hạn                | Input table liên tục tăng                |
| Chạy một lần rồi kết thúc      | Chạy liên tục theo trigger               |
| Output theo một lần chạy       | Output sau từng trigger                  |
| Không cần Kafka offset         | Đọc theo topic / partition / offset      |
| Không cần streaming checkpoint | Có checkpoint để recovery và lưu state   |
| Phù hợp ETL theo lô            | Phù hợp dữ liệu liên tục / gần real-time |

Có thể hình dung:

```text
Batch

CSV
 ↓
Spark
 ↓
Process
 ↓
Output
 ↓
Stop
```

trong khi:

```text
Streaming

Kafka
 ↓
Spark
 ↓
Process
 ↓
Output
 ↓
Kafka có dữ liệu mới
 ↓
Spark tiếp tục process
 ↓
Output
 ↓
...
```

---

# 17. Quan hệ giữa Batch và Streaming trong repo

Phần Streaming không thay thế pipeline Batch mà mở rộng kiến thức từ pipeline hiện tại.

### Batch

Tập trung vào:

```text
validate
deduplicate
join
window
aggregate
partition
performance
```

### Streaming

Tập trung vào:

```text
Kafka
readStream
JSON parsing
schema
validation
partition
offset
trigger
checkpoint
update mode
complete mode
```

Vì vậy repo có thể được dùng để luyện tập theo hướng:

```text
PySpark Batch
      ↓
Join / Window / Partitioning
      ↓
Performance Optimization
      ↓
Kafka
      ↓
Structured Streaming
      ↓
Checkpoint / Offset / Aggregation
```

---

# 18. Cách chạy toàn bộ repo

## Batch

Cài dependency:

```bash
pip install -r requirements.txt
```

Tạo dữ liệu:

```bash
python3 generate_data.py
```

Chạy pipeline:

```bash
python3 pipeline.py
```

Chạy Join lab:

```bash
python3 join_lab.py
```

Chạy Partitioning lab:

```bash
python3 partitioning_lab.py
```

Chạy Performance lab:

```bash
python3 performance_lab.py
```

Hoặc giảm số transaction nếu máy yếu:

```bash
python3 performance_lab.py 300000
```

## Streaming

### Terminal 1 — Kafka

Đảm bảo Kafka broker đang chạy tại:

```text
localhost:9092
```

### Terminal 2 — Spark

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  streaming/streaming_transactions.py \
  --brokers localhost:9092 \
  --topic transactions \
  --starting-offsets earliest \
  --sink console
```

### Terminal 3 — Producer

```bash
python streaming/kafka_producer.py \
  --topic transactions \
  --limit 50 \
  --delay 0.5
```

Luồng chạy thực tế:

```text
Kafka Producer
      ↓
Kafka topic: transactions
      ↓
Spark Structured Streaming
      ↓
JSON parsing
      ↓
Schema
      ↓
Validation
      ↓
Console / Parquet
```

---

# 19. Bài tập tự kiểm tra Structured Streaming

### 1. `earliest` vs `latest`

Đổi:

```text
startingOffsets = earliest
```

sang:

```text
startingOffsets = latest
```

Quan sát số lượng record được đọc khi query bắt đầu.

### 2. Partition

Tạo topic với:

```text
3 partitions
```

sau đó gửi nhiều transaction và quan sát:

```text
kafka_partition
```

### 3. Checkpoint Recovery

Dừng Spark giữa lúc producer đang gửi dữ liệu, sau đó chạy lại với cùng checkpoint.

Quan sát quá trình recovery.

### 4. Update vs Complete

Gửi thêm 10 transaction rồi so sánh output của:

```text
update
```

và:

```text
complete
```

### 5. Xóa checkpoint

Xóa checkpoint rồi chạy lại với:

```text
startingOffsets = earliest
```

So sánh kết quả với trường hợp sử dụng checkpoint cũ.

### 6. Validation

Gửi transaction với:

```text
amount <= 0
```

hoặc thiếu:

```text
customer_id
```

để kiểm tra validation.

---

# 20. Tài liệu giải thích

Lý thuyết và kết quả chi tiết của từng phần:

* [`GIAI_THICH.md`](./GIAI_THICH.md) — giải thích pipeline, Window, dedup, join, bottleneck và `explain()`.
* [`PARTITIONING_LAB_GIAI_THICH.md`](./PARTITIONING_LAB_GIAI_THICH.md) — giải thích `repartition`, `coalesce`, `partitionBy` và small files.
* [`PERFORMANCE_LAB_GIAI_THICH.md`](./PERFORMANCE_LAB_GIAI_THICH.md) — giải thích shuffle, broadcast join, cache/persist, data skew, salting, AQE và physical plan.

---

# 21. Môi trường đã test

```text
Python 3.12
PySpark 4.2.0
OpenJDK 21
```

Batch pipeline chạy với:

```text
local[*]
```

Structured Streaming sử dụng Kafka broker:

```text
localhost:9092
```

Kafka Python producer:

```text
kafka-python-ng
```

Spark Kafka connector trong các lệnh Streaming:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0
```

Các số liệu performance trong repo chỉ có ý nghĩa so sánh tương đối vì được đo trên môi trường local.

---

# 22. License

MIT — dùng tự do cho mục đích học tập.
