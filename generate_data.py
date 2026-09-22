"""
generate_data.py
-----------------
Sinh dữ liệu mẫu customers.csv và transactions.csv, cố tình cài các
trường hợp lỗi/đặc biệt theo yêu cầu:

  - Cùng transaction_id xuất hiện nhiều lần với updated_at khác nhau (duplicate).
  - Có transaction thiếu customer_id (rỗng).
  - Có amount <= 0.
  - Có customer_id không tồn tại trong customers (orphan / không map được).
  - status có SUCCESS, FAILED, PENDING.
  - Một khách hàng có nhiều transaction.
"""

import csv
import random
from datetime import datetime, timedelta

random.seed(42)

import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------
# 1. customers.csv
# ---------------------------------------------------------------------
provinces = ["Hanoi", "HoChiMinh", "DaNang", "HaiPhong", "CanTho"]

customers = []
for i in range(1, 21):  # 20 khách hàng: C0001 -> C0020
    cid = f"C{i:04d}"
    name = f"Customer {i}"
    province = random.choice(provinces)
    created_at = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 300))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    customers.append([cid, name, province, created_at])

with open(f"{OUT_DIR}/customers.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "customer_name", "province", "created_at"])
    w.writerows(customers)

valid_customer_ids = [c[0] for c in customers]

# ---------------------------------------------------------------------
# 2. transactions.csv
# ---------------------------------------------------------------------
statuses = ["SUCCESS", "FAILED", "PENDING"]
rows = []
tx_seq = 1


def new_tx_id():
    global tx_seq
    tid = f"T{tx_seq:05d}"
    tx_seq += 1
    return tid


base_time = datetime(2024, 6, 1)

# --- 2.1 Giao dịch "bình thường" cho từng khách hàng (nhiều tx / khách) ---
for cid in valid_customer_ids:
    n_tx = random.randint(2, 6)  # mỗi khách có nhiều transaction
    for _ in range(n_tx):
        tid = new_tx_id()
        amount = round(random.uniform(10_000, 5_000_000), 2)
        status = random.choice(statuses)
        t_time = base_time + timedelta(
            days=random.randint(0, 90), hours=random.randint(0, 23)
        )
        updated_at = t_time + timedelta(minutes=random.randint(1, 30))
        rows.append([tid, cid, amount, status, t_time, updated_at])

# --- 2.2 Cố tình tạo transaction_id trùng nhau với updated_at khác nhau ---
# Lấy vài transaction hiện có, phát sinh thêm 2-3 bản ghi "update" cho cùng id.
dup_source = random.sample(rows, 4)
for r in dup_source:
    tid, cid, amount, _status, t_time, updated_at = r
    for k in range(1, 3):  # thêm 2 phiên bản cập nhật
        new_status = random.choice(statuses)
        new_amount = round(amount * random.uniform(0.95, 1.05), 2)
        new_updated_at = updated_at + timedelta(hours=k, minutes=random.randint(0, 59))
        rows.append([tid, cid, new_amount, new_status, t_time, new_updated_at])

# --- 2.3 Transaction thiếu customer_id ---
for _ in range(5):
    tid = new_tx_id()
    amount = round(random.uniform(10_000, 1_000_000), 2)
    status = random.choice(statuses)
    t_time = base_time + timedelta(days=random.randint(0, 90))
    updated_at = t_time + timedelta(minutes=5)
    rows.append([tid, "", amount, status, t_time, updated_at])  # customer_id rỗng

# --- 2.4 amount <= 0 ---
for _ in range(5):
    tid = new_tx_id()
    cid = random.choice(valid_customer_ids)
    amount = round(random.choice([0, -1000, -50.5]), 2)
    status = random.choice(statuses)
    t_time = base_time + timedelta(days=random.randint(0, 90))
    updated_at = t_time + timedelta(minutes=5)
    rows.append([tid, cid, amount, status, t_time, updated_at])

# --- 2.5 customer_id không tồn tại trong customers (orphan) ---
for _ in range(4):
    tid = new_tx_id()
    cid = f"C9{random.randint(900, 999)}"  # id không có trong customers.csv
    amount = round(random.uniform(10_000, 500_000), 2)
    status = random.choice(statuses)
    t_time = base_time + timedelta(days=random.randint(0, 90))
    updated_at = t_time + timedelta(minutes=5)
    rows.append([tid, cid, amount, status, t_time, updated_at])

random.shuffle(rows)

with open(f"{OUT_DIR}/transactions.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(
        ["transaction_id", "customer_id", "amount", "status", "transaction_time", "updated_at"]
    )
    for r in rows:
        tid, cid, amount, status, t_time, updated_at = r
        w.writerow(
            [
                tid,
                cid,
                amount,
                status,
                t_time.strftime("%Y-%m-%d %H:%M:%S"),
                updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            ]
        )

print(f"customers.csv: {len(customers)} rows")
print(f"transactions.csv: {len(rows)} rows (bao gồm duplicate/lỗi/orphan có chủ đích)")