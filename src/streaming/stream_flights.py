from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("AviationStream")  \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session created successfully")

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

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "aviation_flights") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.metadata.max.age.ms", "5000") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")


# Debug query to see data in memory
debug_query = json_df.writeStream \
    .format("memory") \
    .queryName("debug_table") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()


query = json_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/aviation/flights") \
    .option("checkpointLocation", "hdfs://namenode:9000/aviation/flights/checkpoint") \
    .outputMode("append") \
    .start()


# Let it run for a bit to collect data
import time
time.sleep(10)

# Query the in-memory table to see the data
result = spark.sql("SELECT * FROM debug_table LIMIT 10")
print(result.show(truncate=False))

query.awaitTermination()
