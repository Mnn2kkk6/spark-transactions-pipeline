"""
join_lab.py
-----------
Bài tập riêng về JOIN — dùng LẠI CHÍNH data/customers.csv và
data/transactions.csv của bài pipeline trước (transactions đóng vai trò
"orders"), KHÔNG sinh data mới.

Mục tiêu: hiểu rõ khác biệt giữa INNER JOIN và LEFT JOIN, và vì sao ETL
thường phải GIỮ LẠI (chứ không âm thầm loại bỏ) những record không map
được sang bảng tham chiếu.

Yêu cầu: đã chạy generate_data.py ít nhất 1 lần trước đó để có sẵn
data/customers.csv và data/transactions.csv trong cùng thư mục.

Chạy:  python join_lab.py   (Windows)
       python3 join_lab.py  (macOS/Linux)
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = f"{BASE}/data"
OUT_DIR = f"{BASE}/join_lab_output"

customers_csv = f"{DATA_DIR}/customers.csv"
orders_csv = f"{DATA_DIR}/transactions.csv"  # dùng transactions.csv làm "orders"

if not (os.path.exists(customers_csv) and os.path.exists(orders_csv)):
    raise FileNotFoundError(
        "Không tìm thấy data/customers.csv hoặc data/transactions.csv. "
        "Hãy chạy `python generate_data.py` (bài pipeline trước) trước khi chạy join_lab.py."
    )

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
os.makedirs(OUT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("join-lab")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================================
# BƯỚC 1: Đọc bằng schema khai báo sẵn (không inferSchema)
#   customers.csv và transactions.csv y hệt bài trước, nên các trường hợp
#   cố ý gài sẵn cũng nguyên vẹn ở đây:
#     - order (transaction) thiếu customer_id (rỗng)
#     - order có customer_id không tồn tại trong customers (C9xx)
#   Cả hai loại này đều là "order không map được customer" theo đúng nghĩa
#   của bài Join, nên ta KHÔNG lọc/dedup gì thêm — giữ nguyên transactions.csv
#   để thấy rõ hành vi join trên dữ liệu thô.
# =====================================================================
customers_schema = StructType([
    StructField("customer_id",   StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("province",      StringType(), True),
    StructField("created_at",    TimestampType(), True),
])

orders_schema = StructType([
    StructField("order_id",      StringType(), True),  # transaction_id -> coi như order_id
    StructField("customer_id",   StringType(), True),
    StructField("amount",        DoubleType(), True),
    StructField("status",        StringType(), True),
    StructField("order_time",    TimestampType(), True),  # transaction_time -> order_time
    StructField("updated_at",    TimestampType(), True),
])

customers_df = spark.read.option("header", True).schema(customers_schema).csv(customers_csv)
orders_df = (
    spark.read.option("header", True).schema(orders_schema).csv(orders_csv)
    # chuẩn hoá: customer_id rỗng "" -> null, để filter/isNull nhất quán
    .withColumn(
        "customer_id",
        F.when(F.trim(F.col("customer_id")) == "", None).otherwise(F.col("customer_id"))
    )
)

n_orders = orders_df.count()
n_customers = customers_df.count()
print(f"Tổng số orders (transactions.csv): {n_orders} | Tổng số customers: {n_customers}")

n_missing_customer_id = orders_df.filter(F.col("customer_id").isNull()).count()
print(f"  trong đó order thiếu customer_id (rỗng): {n_missing_customer_id}")

# =====================================================================
# BƯỚC 2: INNER JOIN
#   Chỉ giữ lại order MAP ĐƯỢC với customer. Order thiếu customer_id
#   hoặc có customer_id không tồn tại (C9xx) sẽ BIẾN MẤT khỏi kết quả,
#   không có cảnh báo gì.
# =====================================================================
inner_join_df = orders_df.join(customers_df, on="customer_id", how="inner")
n_inner = inner_join_df.count()
print(f"\nSố dòng sau INNER JOIN: {n_inner}")

# =====================================================================
# BƯỚC 3: LEFT JOIN
#   Giữ lại TOÀN BỘ order gốc, kể cả order không map được customer.
# =====================================================================
left_join_df = orders_df.join(customers_df, on="customer_id", how="left")
n_left = left_join_df.count()
print(f"Số dòng sau LEFT JOIN: {n_left}")

# =====================================================================
# BƯỚC 4: Tách mapped / unmapped từ kết quả LEFT JOIN
# =====================================================================
mapped_df = left_join_df.filter(F.col("customer_name").isNotNull())
unmapped_df = left_join_df.filter(F.col("customer_name").isNull())

n_mapped = mapped_df.count()
n_unmapped = unmapped_df.count()

print(f"\nMapped: {n_mapped} | Unmapped: {n_unmapped} | Mapped + Unmapped = {n_mapped + n_unmapped}"
      f" (phải bằng tổng orders = {n_orders})")

print("\n=== Unmapped orders (10 dòng đầu): thiếu customer_id hoặc customer_id không tồn tại ===")
unmapped_df.select("order_id", "customer_id", "amount", "status", "order_time") \
    .orderBy("order_id").show(10, truncate=False)

# =====================================================================
# BƯỚC 5: SO SÁNH inner join vs left join
# =====================================================================
missing_from_inner_df = unmapped_df.select("order_id", "customer_id", "amount")

print("\n================= SO SÁNH INNER vs LEFT =================")
print(f"INNER JOIN trả về : {n_inner} dòng")
print(f"LEFT  JOIN trả về : {n_left} dòng")
print(f"Chênh lệch        : {n_left - n_inner} dòng")
print("Đây chính xác là số order INNER JOIN đã âm thầm loại bỏ mà không báo gì.")
print("5 dòng đầu trong số đó:")
missing_from_inner_df.show(5, truncate=False)

assert n_left == n_orders, "LEFT JOIN phải giữ đủ số dòng orders gốc"
assert n_inner == n_mapped, "INNER JOIN phải bằng đúng số order map được customer"
assert n_inner + n_unmapped == n_orders, "inner + unmapped phải bằng tổng orders"
print("\n[OK] Các phép kiểm tra số liệu đều khớp (assert pass).")

# =====================================================================
# BƯỚC 6: Ghi mapped/unmapped ra để tiện xem lại
# =====================================================================
mapped_df.write.mode("overwrite").parquet(f"{OUT_DIR}/mapped_orders_parquet")
unmapped_df.select("order_id", "customer_id", "amount", "status", "order_time") \
    .write.mode("overwrite").parquet(f"{OUT_DIR}/unmapped_orders_parquet")

print(f"\nĐã ghi kết quả vào: {OUT_DIR}")

# =====================================================================
# BƯỚC 7: explain() — inner join vs left join khác nhau thế nào trong plan
# =====================================================================
print("\n================= EXPLAIN: INNER JOIN =================")
inner_join_df.explain(mode="formatted")

print("\n================= EXPLAIN: LEFT JOIN =================")
left_join_df.explain(mode="formatted")

spark.stop()
print("\nDONE.")
