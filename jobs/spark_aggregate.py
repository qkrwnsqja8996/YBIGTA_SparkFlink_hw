#!/usr/bin/env python3
"""
Spark Aggregation Job - YBIGTA Spark & Flink HA Homework
=========================================================
Reads events from /data/spark-input/events.csv, aggregates by event_type,
and writes results to /data/spark-output/result.csv.

Input CSV columns: event_type, value, timestamp
Output CSV columns: event_type, total_value, event_count, max_timestamp

The number of shuffle partitions is controlled by spark.sql.shuffle.partitions
in spark-defaults.conf. Tuning this is required for Scenario 07.

Usage:
    spark-submit /jobs/spark_aggregate.py
"""

import sys
import os
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger('spark_aggregate')


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    logger.info("Starting Spark aggregation job")

    spark = SparkSession.builder \
        .appName("spark-aggregate") \
        .getOrCreate()

    # Log effective shuffle partitions
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions", "200")
    logger.info("Effective spark.sql.shuffle.partitions: %s", shuffle_partitions)

    input_path = "/data/spark-input/events.csv"
    output_path = "/data/spark-output/result.csv"

    # Check input exists
    if not os.path.exists(input_path):
        logger.error("Input file not found: %s", input_path)
        logger.error("Run './scripts/cluster.sh init' to load test data first.")
        sys.exit(1)

    logger.info("Reading input: %s", input_path)

    # Read CSV
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    # Validate schema
    expected_cols = {'event_type', 'value', 'timestamp'}
    actual_cols = set(df.columns)
    if not expected_cols.issubset(actual_cols):
        missing = expected_cols - actual_cols
        logger.error("Input CSV is missing columns: %s", missing)
        logger.error("Found columns: %s", df.columns)
        sys.exit(1)

    logger.info("Input schema: %s", df.schema.simpleString())

    # Aggregate (count 호출 최소화 - 불필요한 action은 메모리 낭비)
    result = df.groupBy("event_type") \
        .agg(
            F.sum("value").alias("total_value"),
            F.count("*").alias("event_count"),
            F.max("timestamp").alias("max_timestamp")
        ) \
        .orderBy("event_type")

    # Write output as single CSV (coalesce to 1 partition for deterministic output)
    tmp_output = output_path + "_tmp"
    result.coalesce(1).write.csv(
        tmp_output,
        header=True,
        mode="overwrite"
    )

    # Move from tmp dir to final path
    import glob
    import shutil

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Find the part file
    part_files = glob.glob(os.path.join(tmp_output, "part-*.csv"))
    if not part_files:
        logger.error("No output part file found in %s", tmp_output)
        sys.exit(1)

    shutil.move(part_files[0], output_path)
    shutil.rmtree(tmp_output, ignore_errors=True)

    logger.info("Output written to: %s", output_path)
    spark.stop()
    logger.info("Spark aggregation job completed successfully")


if __name__ == '__main__':
    main()
