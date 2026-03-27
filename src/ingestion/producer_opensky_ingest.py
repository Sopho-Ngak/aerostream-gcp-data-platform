import requests
import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "aviation_flights"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
OPEN_SKY_URL = "https://opensky-network.org/api/states/all"
MAX_RETRIES = 30
RETRY_INTERVAL = 5

def create_kafka_producer():
    """Create Kafka producer with retry logic"""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_SERVER} (attempt {retries + 1}/{MAX_RETRIES})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_block_ms=30000,  # Increase timeout
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
                connections_max_idle_ms=540000
            )
            # Test the connection
            producer.bootstrap_connected()
            logger.info("✅ Successfully connected to Kafka")
            return producer
        except NoBrokersAvailable as e:
            retries += 1
            if retries < MAX_RETRIES:
                logger.warning(f"⚠️ Kafka not ready yet: {e}. Retrying in {RETRY_INTERVAL} seconds...")
                time.sleep(RETRY_INTERVAL)
            else:
                logger.error(f"❌ Failed to connect to Kafka after {MAX_RETRIES} attempts")
                raise
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to Kafka: {e}")
            raise

def fetch_flight_data():
    try:
        response = requests.get(OPEN_SKY_URL)
        response.raise_for_status()
        data = response.json()
        return data.get("states", [])
    except requests.RequestException as e:
        logger.error(f"❌ Error fetching flight data: {e}")
        return []

def stream_flights():
    # Create producer with retry logic
    producer = create_kafka_producer()
    
    # Ensure topic exists (optional - Kafka will auto-create if enabled)
    try:
        producer.send(KAFKA_TOPIC, value={"test": "connection"}).get(timeout=10)
        logger.info(f"✅ Successfully sent test message to topic {KAFKA_TOPIC}")
    except Exception as e:
        logger.warning(f"⚠️ Could not send test message: {e}")
    
    while True:
        try:
            flights = fetch_flight_data()
            if flights:
                for flight in flights:
                    if flight:  # Check if flight data exists
                        data = {
                            "icao24": flight[0],
                            "callsign": flight[1].strip() if flight[1] else None,
                            "origin_country": flight[2],
                            "time_position": flight[3],
                            "last_contact": flight[4],
                            "longitude": flight[5],
                            "latitude": flight[6],
                            "baro_altitude": flight[7],
                            "on_ground": flight[8],
                            "velocity": flight[9],
                            "true_track": flight[10],
                            "vertical_rate": flight[11],
                            "sensors": flight[12],
                            "geo_altitude": flight[13],
                            "squawk": flight[14],
                            "spi": flight[15],
                            "position_source": flight[16]
                        }
                        future = producer.send(KAFKA_TOPIC, value=data)
                        # Optional: wait for send confirmation
                        # future.get(timeout=10)
                
                producer.flush()
                logger.info(f"Sent {len(flights)} flight records")
            else:
                logger.warning("No flight data received")
            
            time.sleep(120)  # Fetch new data every 10 seconds
            
        except Exception as e:
            logger.error(f"Error in streaming loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    stream_flights()