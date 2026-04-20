"""
Batch job: read raw flight Parquet from GCS (Kafka streaming sink), aggregate metrics,
write partitioned Parquet back to GCS for direct BigQuery load.

Run by Airflow SparkSubmitOperator with --processing_date YYYY-MM-DD.
"""
from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Flight batch transformations")
    p.add_argument("--processing_date", required=True, help="Partition date YYYY-MM-DD")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ds = args.processing_date
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    raw_path = f"gs://{gcs_bucket}/aviation/flights/raw"
    out_path = f"gs://{gcs_bucket}/flight_metrics/ingestion_date={ds}"

    # Under spark-submit the JVM already created a SparkContext; reuse it.
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    parquet_glob = f"{raw_path}/*.parquet"
    try:
        logger.info("Reading raw parquet from: %s", parquet_glob)
        df = spark.read.parquet(parquet_glob)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read raw parquet from {parquet_glob}: {exc}"
        ) from exc

    source_count = df.count()
    logger.info("Read %s records from raw path: %s", source_count, raw_path)

    if source_count == 0:
        raise RuntimeError(
            f"Raw input exists but has 0 rows for processing_date={ds} at {raw_path}. "
            "Failing fast to avoid exporting empty parquet downstream."
        )

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

    output_count = out.count()
    if output_count == 0:
        raise RuntimeError(
            f"Transformed output has 0 rows for processing_date={ds}. "
            "Failing fast to prevent empty downstream loads."
        )
    logger.info("Writing %s transformed records to %s", output_count, out_path)

    out.write.mode("overwrite").parquet(out_path)
    spark.stop()


if __name__ == "__main__":
    main()
