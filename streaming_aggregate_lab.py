"""
Output mode lab for Spark Structured Streaming + Kafka.

The aggregation is intentionally simple so append/update/complete can be observed.
For a grouped aggregation:
  - update: only groups changed in the current trigger are shown
  - complete: the entire current result table is shown each trigger

Append is not used here because the aggregate rows are expected to change.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def parse_args():
    parser = argparse.ArgumentParser(description="Structured Streaming output-mode lab")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--mode", choices=["update", "complete"], default="update")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    return parser.parse_args()


def main():
    args = parse_args()
    checkpoint = args.checkpoint or f"output_streaming/checkpoint_aggregate_{args.mode}"

    spark = (
        SparkSession.builder
        .appName(f"transactions-output-mode-{args.mode}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_time", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("order_date", StringType(), True),
    ])

    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json("json_value", schema).alias("data"))
        .select("data.*")
        .withColumn("transaction_time", F.to_timestamp("transaction_time"))
    )

    valid_events = events.filter(
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
    )

    # Keep the aggregation intentionally simple for the output-mode experiment.
    # A plain groupBy aggregation is useful here because its rows can be updated
    # as new Kafka events arrive.
    aggregates = (
        valid_events
        .groupBy("status")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
    )

    query = (
        aggregates.writeStream
        .outputMode(args.mode)
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )

    print("=== Output mode lab started ===")
    print(f"Mode: {args.mode}")
    print(f"Checkpoint: {checkpoint}")
    print("Send more Kafka events and compare what changes between update and complete.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
