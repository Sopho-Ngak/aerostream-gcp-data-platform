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


def rename_output_parquet_files(spark: SparkSession, out_path: str, ds: str, logger: logging.Logger) -> None:
    """Rename Spark part files to deterministic, descriptive parquet filenames."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    out_uri = jvm.java.net.URI(out_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(out_uri, hadoop_conf)
    out_dir = jvm.org.apache.hadoop.fs.Path(out_path)

    statuses = fs.listStatus(out_dir)
    part_names = []
    for status in statuses:
        if not status.isFile():
            continue
        file_name = status.getPath().getName()
        if file_name.startswith("part-") and file_name.endswith(".parquet"):
            part_names.append(file_name)

    part_names.sort()
    if not part_names:
        raise RuntimeError(f"No parquet part files found to rename in output path: {out_path}")

    for idx, old_name in enumerate(part_names, start=1):
        new_name = f"flight_metrics_{ds}_{idx:03d}.parquet"
        src = jvm.org.apache.hadoop.fs.Path(out_dir, old_name)
        dst = jvm.org.apache.hadoop.fs.Path(out_dir, new_name)
        if fs.exists(dst):
            fs.delete(dst, False)
        renamed = fs.rename(src, dst)
        if not renamed:
            raise RuntimeError(f"Failed to rename {old_name} to {new_name} in {out_path}")

    logger.info("Renamed %s parquet part file(s) with descriptive names in %s", len(part_names), out_path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Flight batch transformations")
    p.add_argument("--processing_date", required=True, help="Partition date YYYY-MM-DD")
    p.add_argument(
        "--input_path",
        required=False,
        help="Optional fully-qualified gs:// path of a single parquet file to process",
    )
    return p.parse_args()


def resolve_latest_parquet_path(spark: SparkSession, raw_path: str, logger: logging.Logger) -> str:
    """Resolve the newest parquet file under raw_path using Hadoop FS metadata."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    raw_uri = jvm.java.net.URI(raw_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(raw_uri, hadoop_conf)
    raw_dir = jvm.org.apache.hadoop.fs.Path(raw_path)

    if not fs.exists(raw_dir):
        raise RuntimeError(f"Raw path does not exist: {raw_path}")

    statuses = fs.listStatus(raw_dir)
    latest_path = None
    latest_mtime = -1
    parquet_count = 0

    for status in statuses:
        if not status.isFile():
            continue
        file_name = status.getPath().getName()
        if not file_name.endswith(".parquet"):
            continue
        if status.getLen() == 0:
            continue

        parquet_count += 1
        mtime = status.getModificationTime()
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = status.getPath().toString()

    if not latest_path:
        raise RuntimeError(f"No parquet files found under raw path: {raw_path}")

    logger.info(
        "Discovered %s parquet file(s) under %s; selected latest file: %s",
        parquet_count,
        raw_path,
        latest_path,
    )
    return latest_path


def main() -> None:
    args = parse_args()
    ds = args.processing_date
    input_path = args.input_path
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    raw_path = f"gs://{gcs_bucket}/aviation/flights/raw"
    out_path = f"gs://{gcs_bucket}/flight_metrics/ingestion_date={ds}"

    # Under spark-submit the JVM already created a SparkContext; reuse it.
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    read_path = input_path
    if not read_path:
        read_path = resolve_latest_parquet_path(spark=spark, raw_path=raw_path, logger=logger)

    try:
        logger.info("Reading raw parquet from: %s", read_path)
        df = spark.read.parquet(read_path)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read raw parquet from {read_path}: {exc}"
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
    rename_output_parquet_files(spark=spark, out_path=out_path, ds=ds, logger=logger)
    spark.stop()


if __name__ == "__main__":
    main()
