"""
Simulates an AWS Lambda / Event-Driven Producer pushing live weather 
data into Apache Kafka by reusing fetch functions from src.ingestion.weather_bronze_ingestion.
"""
import json
import os
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path for relative imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer
from src.ingestion.weather_bronze_ingestion import (
    fetch_open_meteo,
    get_configured_session,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("kafka_weather_producer")


def json_serializer(data: dict) -> bytes:
    """Serializes Python dictionary to UTF-8 encoded JSON bytes."""
    return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")


def run_kafka_producer(
    locations: list,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "weather_raw_events"
) -> None:
    """
    Fetches live weather records using src.ingestion modules and pushes 
    them as events into the target Kafka topic.
    """
    logger.info("Initializing Kafka Producer connecting to: %s", bootstrap_servers)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=json_serializer,
            api_version=(7, 4, 0),
            acks="all",  # Ensure leader and replicas acknowledge
            retries=3
        )
    except Exception as err:
        logger.error("Failed to connect to Kafka broker at %s: %s", bootstrap_servers, err)
        raise

    session = get_configured_session(retries=3, backoff_factor=1.0)
    logger.info("Starting live event publishing to topic: '%s'", topic)

    for loc in locations:
        code = loc["code"]
        lat = loc["lat"]
        lon = loc["lon"]

        logger.info("Fetching live payload for %s (%f, %f)...", code, lat, lon)
        record = fetch_open_meteo(session=session, location_code=code, lat=lat, lon=lon)
        
        # Inject Kafka event timestamp metadata
        record["event_published_at_utc"] = datetime.now(timezone.utc).isoformat()

        # Publish to Kafka topic
        future = producer.send(topic, value=record)
        result = future.get(timeout=10)  # Synchronous wait for confirmation
        
        logger.info(
            "Successfully published %s event to Kafka [Topic: %s | Partition: %d | Offset: %d]",
            code, result.topic, result.partition, result.offset
        )
        time.sleep(1)  # Simulate event delay

    producer.flush()
    producer.close()
    logger.info("All location events pushed to Kafka successfully.")


if __name__ == "__main__":
    SAMPLE_LOCATIONS = [
        {"code": "BOM", "lat": 19.0896, "lon": 72.8656},
        {"code": "DEL", "lat": 28.5562, "lon": 77.1000},
        {"code": "BLR", "lat": 13.1986, "lon": 77.7066},
        {"code": "PNQ", "lat": 18.5821, "lon": 73.9197}
    ]

    # Use environment variable with fallback to container service name 'kafka'
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    TARGET_TOPIC = "weather_raw_events"

    run_kafka_producer(
        locations=SAMPLE_LOCATIONS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=TARGET_TOPIC
    )