# Partitioning & Performance Lab (PySpark)

File thực hành: `partitioning_lab.py` — **dùng lại đúng dữ liệu của bài pipeline trước**
(`data/customers.csv` + `data/transactions.csv`), không sinh file `orders.csv` riêng.

"orders" ở đây = `transactions.csv` LEFT JOIN `customers.csv` để lấy thêm cột `province`
(`transactions.csv` gốc không có sẵn province, chỉ có `customer_id`). Cột `order_date`
cũng được bổ sung ngay trong `generate_data.py` (phần ngày của `transaction_time`), nên
`transactions.csv` giờ có thêm cột `order_date` ở cuối so với bài pipeline gốc.

```bash
python generate_data.py     # nếu chưa có data/customers.csv, data/transactions.csv
python partitioning_lab.py  # Windows: python ...   macOS/Linux: python3 ...
```

> Script tắt Adaptive Query Execution (`spark.sql.adaptive.enabled=false`) và cố định
> `spark.sql.shuffle.partitions=8`. Lý do: Spark 3.x/4.x mặc định bật AQE, sẽ **tự động
> gộp** các partition nhỏ sau shuffle lại để tối ưu hiệu năng thực tế — với dataset nhỏ
> (106 dòng) AQE sẽ gộp gần hết về 1 partition, che mất đúng hành vi cơ bản của
> `repartition`/`coalesce` mà bài học muốn minh hoạ. Tắt AQE để số partition hiển thị
> đúng lý thuyết (khi làm việc thật với dataset lớn, nên **để AQE bật**).

## Dữ liệu dùng lại

- 106 "orders" (= transactions.csv), 20 customers, 5 province (Hanoi, HoChiMinh, DaNang,
  HaiPhong, CanTho).
- **9 dòng có `province = null`** — vì đây chính là 9 order không map được customer đã
  cố ý gài ở bài trước (5 thiếu `customer_id`, 4 `customer_id` không tồn tại). Bài
  Partitioning không lọc bỏ chúng, giữ nguyên để minh hoạ Spark/Parquet xử lý giá trị
  null trong cột dùng làm partition key như thế nào (xem phần `partitionBy` bên dưới).

## Kết quả chạy thật

Số partition ban đầu (sau join, trước khi làm gì thêm): **1**.

| Case | Số partition | Số dòng/partition | Số file `part-*.parquet` |
|---|---|---|---|
| `repartition(2)` | 2 | cân bằng đều | 2 |
| `repartition(4)` | 4 | cân bằng đều | 4 |
| `repartition(8)` | 8 | cân bằng đều | 8 |
| `repartition("province")` | 8 | **lệch** — 4 partition có dữ liệu (gồm cả bucket cho null), 4 partition rỗng | 4 |
| `coalesce(2)` từ df gốc (1 partition) | **vẫn là 1** | không đổi | 1 |
| `repartition(8).coalesce(2)` | 2 | | 2 |

Kết quả `groupBy("province")` (total_orders, total_amount, avg_amount) **giống hệt nhau**
ở mọi case — repartition/coalesce chỉ thay đổi cách dữ liệu được chia vật lý giữa các
partition, không đổi nội dung logic hay kết quả tính toán.

### Vì sao `coalesce(2)` từ df gốc vẫn ra 1 partition?

`coalesce()` **chỉ có thể giảm** số partition (gộp partition liền kề, không shuffle toàn
bộ), **không thể tăng**. Vì `orders_df` gốc chỉ có 1 partition, `coalesce(2)` không có gì
để gộp thêm nên giữ nguyên 1. Case `repartition(8).coalesce(2)` mới minh hoạ đúng việc
coalesce giảm thật sự: 8 → 2.

## So sánh WRITE: không `partitionBy` vs có `partitionBy("province")`

Cùng ghi `orders_df.repartition(4)`:

**Không `partitionBy`** — output phẳng, 4 file:
```
write_no_partitionby/
├── _SUCCESS
├── part-00000-....snappy.parquet
├── part-00001-....snappy.parquet
├── part-00002-....snappy.parquet
└── part-00003-....snappy.parquet
```

**Có `partitionBy("province")`** — chia thành thư mục con theo giá trị province, **tổng
21 file** (thay vì 4×5=20 vì province phân bố không đều giữa 4 partition):
```
write_with_partitionby/
├── _SUCCESS
├── province=CanTho/                     -> 4 file
├── province=DaNang/                     -> 4 file
├── province=HaiPhong/                   -> 3 file
├── province=Hanoi/                      -> 4 file
├── province=HoChiMinh/                  -> 4 file
└── province=__HIVE_DEFAULT_PARTITION__/ -> 2 file   <-- các dòng có province = null
```

**Điểm đáng chú ý**: Spark/Hive quy ước gom mọi dòng có giá trị **null** ở cột dùng làm
`partitionBy` vào một thư mục đặc biệt tên `__HIVE_DEFAULT_PARTITION__`, thay vì loại bỏ
hay báo lỗi. Đây là ví dụ thực tế cho lý do bài trước phải tách `unmapped_df` riêng: nếu
không xử lý sớm, những order "không rõ province" vẫn lặng lẽ tồn tại trong output, dễ bị
bỏ sót khi ai đó chỉ quét các thư mục `province=<tên tỉnh cụ thể>/`.

**Giải thích chung**: `partitionBy(col)` khi ghi tạo cấu trúc thư mục kiểu Hive
(`col=value/`), cho phép engine đọc sau này áp dụng **partition pruning** — chỉ đọc đúng
thư mục cần (`WHERE province = 'Hanoi'` chỉ quét `province=Hanoi/`) thay vì đọc hết rồi
lọc. Cái giá: số file vật lý tăng lên vì **mỗi Spark partition ghi riêng 1 file cho mỗi
giá trị partition-column nó chứa** — Spark partition × số giá trị cột phân vùng càng
lớn, số file sinh ra càng nhiều.

## Nâng cao: `repartition(50)` trên dataset nhỏ (106 dòng)

```
repartition(50) trên 106 dòng -> 50 partition, 50 file part-*.parquet
Kích thước mỗi file: ~1.8 KB (nhỏ nhất 1.78 KB, lớn nhất 1.99 KB)
```

50 partition cho 106 dòng = trung bình ~2 dòng/partition, mỗi file Parquet chỉ **~1.8
KB** — nhỏ hơn hàng chục nghìn lần so với khuyến nghị thực tế cho 1 file Parquet (thường
128 MB – 1 GB/file). Đây chính là hiện tượng **"small files problem"**, càng rõ hơn nữa
vì dataset gốc chỉ có 106 dòng.

### Vì sao quá nhiều file nhỏ gây bất lợi

1. **Overhead metadata**: mỗi file Parquet có header/footer, schema, thống kê (min/max,
   row group...) riêng — với hàng nghìn file nhỏ, tổng overhead này lớn hơn cả dữ liệu
   thật.
2. **Tốn chi phí liệt kê & mở file**: mỗi lần list/open/close 1 file đều tốn thời gian;
   với object storage (S3, GCS) mỗi request có độ trễ cố định — hàng nghìn file nhỏ =
   hàng nghìn request, cộng dồn thành thời gian đọc lớn dù tổng dung lượng nhỏ.
3. **Quá tải NameNode (nếu dùng HDFS)**: NameNode giữ metadata mọi file trong bộ nhớ;
   quá nhiều file nhỏ làm phình bộ nhớ NameNode, ảnh hưởng cả cluster.
4. **Giảm hiệu quả nén & columnar scan**: Parquet tối ưu khi mỗi file/row group đủ lớn
   để nén tốt và tận dụng predicate pushdown; file quá nhỏ mất lợi thế này.
5. **Task overhead khi đọc lại**: đọc dataset gồm rất nhiều file nhỏ tạo ra rất nhiều
   task nhỏ, overhead khởi tạo task có thể lớn hơn cả thời gian xử lý thật.

Ngược lại, quá ít partition/file (1 file cho dataset hàng trăm GB) cũng dở — mất khả
năng xử lý song song. Cần cân bằng, thường nhắm **128 MB – 1 GB/file**.

---

## Trả lời các câu hỏi cuối bài

### 1. `repartition` khác `coalesce` như thế nào?

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Có shuffle? | **Có** — shuffle toàn bộ dữ liệu, phân bổ lại đều | **Không** (mặc định) — chỉ gộp partition hiện có |
| Tăng số partition? | Được — tăng hoặc giảm tuỳ ý | **Không** — chỉ giảm (hoặc giữ nguyên) |
| Cân bằng dữ liệu (không truyền cột) | Đều nhau (round-robin) | Không đảm bảo — có thể lệch nếu dữ liệu gốc đã lệch |
| Chi phí | Cao hơn (shuffle tốn CPU/network/disk) | Rẻ hơn nhiều |
| Dùng khi nào | Cần TĂNG partition, cần chia đều, hoặc cần gom theo 1 cột trước khi join/groupBy | Chỉ cần GIẢM partition trước khi ghi output |

Trong bài lab: `coalesce(2)` từ df gốc (1 partition) không làm được gì; phải xuất phát
từ `repartition(8)` trước thì `coalesce(2)` mới thực sự giảm được (8 → 2).

### 2. `repartition` theo column khác gì `repartition` theo số?

- `repartition(n)` (theo số): **RoundRobinPartitioning** — chia đều số dòng vào `n`
  partition kiểu xoay vòng, không quan tâm nội dung. Luôn cân bằng số dòng.
- `repartition(col)` (theo cột): **HashPartitioning** — băm giá trị cột, đảm bảo cùng
  giá trị luôn nằm cùng 1 partition (quan trọng cho groupBy/join sau đó theo cùng cột,
  tránh shuffle lại lần nữa). Cái giá: nếu giá trị cột phân bố lệch (như 9 dòng
  `province=null` dồn vào cùng 1 bucket, cộng thêm các tỉnh có số đơn khác nhau),
  partition cũng lệch theo — không đảm bảo cân bằng số dòng.

### 3. `partitionBy` khi write khác `repartition` trong Spark ở điểm nào?

- `repartition`/`coalesce`: transformation **trong lúc tính toán**, quyết định dữ liệu
  chia thành bao nhiêu partition khi xử lý (ảnh hưởng số task song song, shuffle...).
- `partitionBy(col)`: tham số **khi GHI**, quyết định cấu trúc **thư mục vật lý trên
  đĩa** (`col=value/`), phục vụ đọc lại hiệu quả sau này (partition pruning).
- Hai khái niệm độc lập nhưng cộng hưởng: số Spark partition trước khi ghi × số giá trị
  khác nhau của cột `partitionBy` ≈ số file thực tế. Trong bài lab: 4 Spark partition ×
  6 giá trị (5 tỉnh + 1 bucket null) → tối đa 24, thực tế ra 21 file (một số
  partition không chứa đủ mọi giá trị). Muốn ít file hơn mỗi thư mục, nên
  `repartition("province")` **trước khi** `partitionBy("province")` khi ghi.

### 4. Trường hợp nào nên tăng/giảm số partition?

**Nên TĂNG** (`repartition` lên số lớn hơn):
- Dữ liệu vào ít partition nhưng dung lượng lớn → tăng để tận dụng xử lý song song.
- Trước khi join/groupBy nặng mà dữ liệu đang dồn vào quá ít partition (dễ OOM 1 task).
- Cần cân bằng lại dữ liệu bị lệch nặng sau một phép biến đổi trước đó.

**Nên GIẢM** (`coalesce`, hoặc `repartition` xuống thấp):
- Sau khi filter/aggregate làm dữ liệu co lại nhiều mà vẫn giữ nguyên số partition ban
  đầu → gộp lại tránh sinh nhiều file nhỏ khi ghi.
- Trước khi ghi output cuối, muốn kiểm soát số file cho gọn → `coalesce(n)` rẻ hơn
  `repartition(n)` nếu chỉ cần giảm.
- Khi số partition vượt xa số core khả dụng, gây overhead scheduling không cần thiết.

### 5. Vì sao không nên tạo quá nhiều file nhỏ?

Vì (tóm tắt phần "Nâng cao" ở trên): (1) overhead metadata/footer trên mỗi file
Parquet, (2) chi phí list/open/close file — tốn kém đặc biệt với object storage do độ
trễ mỗi request, (3) quá tải metadata NameNode nếu dùng HDFS, (4) mất lợi thế nén và
columnar scan của Parquet khi file/row group quá nhỏ, (5) sinh quá nhiều task nhỏ khi
đọc lại, overhead khởi tạo task lớn hơn cả thời gian xử lý thật. Ngược lại cũng không
nên dồn hết vào 1 file quá lớn — mất khả năng xử lý song song. Best practice: nhắm kích
thước mỗi file khoảng **128 MB – 1 GB**, điều chỉnh `repartition`/`coalesce` trước khi
ghi cho phù hợp với dung lượng dữ liệu thực tế.
