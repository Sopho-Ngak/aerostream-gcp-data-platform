import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCS_BUCKET = os.environ["GCS_BUCKET_NAME"]

spark = SparkSession.builder \
    .appName("AviationStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info("✅ Spark session created successfully")

schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("sensors", ArrayType(IntegerType()), True),
    StructField("geo_altitude", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", BooleanType(), True),
    StructField("position_source", IntegerType(), True)
])

logger.info("✅ Schema defined successfully")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "aviation_flights") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.metadata.max.age.ms", "5000") \
    .option("kafkaConsumer.pollTimeoutMs", "120000") \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.max.block.ms", "60000") \
    .option("maxOffsetsPerTrigger", "20000") \
    .load()


json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")


query = json_df.writeStream \
    .format("parquet") \
    .option("path", f"gs://{GCS_BUCKET}/aviation/flights/raw") \
    .option("checkpointLocation", f"gs://{GCS_BUCKET}/aviation/checkpoints/flights") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

logger.info(f"✅ Streaming query started, writing parquet to gs://{GCS_BUCKET}/aviation/flights/raw every 30s")
query.awaitTermination()
