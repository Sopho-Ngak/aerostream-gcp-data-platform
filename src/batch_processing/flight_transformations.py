"""
Batch job: read raw flight Parquet from HDFS (Kafka streaming sink), aggregate metrics, write partitioned Parquet.

Run by Airflow SparkSubmitOperator with --processing_date YYYY-MM-DD.
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Flight batch transformations")
    p.add_argument("--processing_date", required=True, help="Partition date YYYY-MM-DD")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ds = args.processing_date

    # Under spark-submit the JVM already created a SparkContext; reuse it (--name sets the app name).
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_candidates = [
        "hdfs://namenode:9000/aviation/flights",
        "hdfs://namenode:9000/tmp/aviation/flights",
    ]
    # /tmp is generally writable for non-superusers in HDFS dev setups.
    out_path = f"hdfs://namenode:9000/tmp/aviation/processed/flight_metrics/ingestion_date={ds}"

    empty_schema = StructType(
        [
            StructField("icao24", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("ingestion_date", StringType(), True),
            StructField("altitude_km", DoubleType(), True),
            StructField("speed_kmh", DoubleType(), True),
        ]
    )

    df = None
    for raw_path in raw_candidates:
        try:
            df = spark.read.parquet(raw_path)
            break
        except Exception:
            continue
    if df is None:
        df = spark.createDataFrame([], empty_schema)

    if df.limit(1).count() == 0:
        out = spark.createDataFrame([], empty_schema)
    else:
        # Align with streaming JSON fields (see src/streaming/stream_flights.py)
        base = (
            df.withColumn("ingestion_date", lit(ds))
            .withColumn(
                "altitude_km",
                when(col("baro_altitude").isNotNull(), col("baro_altitude") / 1000.0).otherwise(None),
            )
            .withColumn(
                "speed_kmh",
                when(col("velocity").isNotNull(), col("velocity") * 3.6).otherwise(None),
            )
        )
        out = base.select(
            col("icao24"),
            col("origin_country"),
            col("ingestion_date"),
            col("altitude_km"),
            col("speed_kmh"),
        )

    out.write.mode("overwrite").parquet(out_path)
    spark.stop()


if __name__ == "__main__":
    main()
