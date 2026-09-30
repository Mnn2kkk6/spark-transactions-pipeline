"""
Spark Transactions - Structured Streaming + Kafka
---------------------------------------------------
Flow:
    Kafka topic -> parse JSON -> validate -> select valid events -> console/parquet sink

Schema follows data/transactions.csv in the main batch pipeline.
Run with spark-submit and the Spark Kafka connector.
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka -> Spark Structured Streaming lab")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--checkpoint", default="output_streaming/checkpoint_transactions")
    parser.add_argument("--output", default="output_streaming/valid_transactions")
    parser.add_argument("--sink", choices=["console", "parquet"], default="console")
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("transactions-structured-streaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_time", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("order_date", StringType(), True),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka value is binary; decode it and parse the JSON transaction.
    parsed_df = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", F.from_json(F.col("json_value"), transaction_schema))
        .select("*", "data.*")
        .drop("data", "json_value")
        .withColumn("transaction_time", F.to_timestamp("transaction_time"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

    is_missing_customer = (
        F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")
    )
    is_invalid_amount = F.col("amount").isNull() | (F.col("amount") <= 0)

    result_df = (
        parsed_df
        .withColumn(
            "error_reason",
            F.when(
                is_missing_customer & is_invalid_amount,
                F.lit("MISSING_CUSTOMER_ID;INVALID_AMOUNT"),
            )
            .when(is_missing_customer, F.lit("MISSING_CUSTOMER_ID"))
            .when(is_invalid_amount, F.lit("INVALID_AMOUNT"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("is_valid", (~(is_missing_customer | is_invalid_amount)))
    )

    # For the first streaming lab we keep valid events append-only.
    # Kafka partition/offset are retained so each event can be traced back to the source.
    output_df = result_df.filter(F.col("is_valid")).select(
        "transaction_id",
        "customer_id",
        "amount",
        "status",
        "transaction_time",
        "updated_at",
        "order_date",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )

    writer = (
        output_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
    )

    if args.sink == "console":
        query = (
            writer
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .start()
        )
    else:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        query = writer.format("parquet").option("path", args.output).start()

    print("=== Structured Streaming started ===")
    print(f"Kafka: {args.brokers}, topic: {args.topic}")
    print(f"Starting offsets: {args.starting_offsets}")
    print(f"Checkpoint: {args.checkpoint}")
    print(f"Sink: {args.sink}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
