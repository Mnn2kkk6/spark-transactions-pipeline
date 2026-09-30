"""
Publish the repository's data/transactions.csv into a Kafka topic as JSON.

Dependency:
    pip install kafka-python-ng
"""

import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(description="CSV -> Kafka producer")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument(
        "--csv", default=str(Path(__file__).resolve().parents[1] / "data" / "transactions.csv")
    )
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--limit", type=int, default=0, help="0 means all rows")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )

    sent = 0
    with open(args.csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get("transaction_id") or None
            # Convert amount from CSV text to numeric JSON value.
            if row.get("amount") not in (None, ""):
                row["amount"] = float(row["amount"])

            producer.send(args.topic, key=key, value=row)
            sent += 1
            print(f"sent #{sent}: {key}")

            if sent % 20 == 0:
                producer.flush()
            if args.delay > 0:
                time.sleep(args.delay)
            if args.limit and sent >= args.limit:
                break

    producer.flush()
    producer.close()
    print(f"Done. Sent {sent} messages to topic '{args.topic}'.")


if __name__ == "__main__":
    main()
