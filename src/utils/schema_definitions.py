"""
Shared schema definitions for PySpark and other components
"""

from pyspark.sql.types import *

# Flight data schema (matches OpenSky API)
flight_schema = StructType([
    StructField("event_id", StringType(), True),
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
    StructField("position_source", IntegerType(), True),
    StructField("ingestion_timestamp", LongType(), True),
    StructField("ingestion_date", StringType(), True),
    StructField("ingestion_hour", StringType(), True)
])

# Aggregated metrics schema
country_metrics_schema = StructType([
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("origin_country", StringType(), True),
    StructField("flight_count", LongType(), True),
    StructField("avg_altitude", DoubleType(), True),
    StructField("avg_speed", DoubleType(), True),
    StructField("grounded_count", LongType(), True),
    StructField("airborne_count", LongType(), True),
    StructField("processing_time", TimestampType(), True)
])

# Anomaly detection schema
anomaly_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("detection_time", TimestampType(), True)
])

# Helper functions to get schemas
def get_flight_schema():
    return flight_schema

def get_country_metrics_schema():
    return country_metrics_schema

def get_anomaly_schema():
    return anomaly_schema

# Schema for different file formats
def get_schema_for_format(format_type: str):
    schemas = {
        "flight": flight_schema,
        "country_metrics": country_metrics_schema,
        "anomaly": anomaly_schema
    }
    return schemas.get(format_type, flight_schema)