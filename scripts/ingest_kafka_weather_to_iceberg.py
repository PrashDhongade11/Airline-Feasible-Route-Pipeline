import sys
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql.functions import col, coalesce, current_timestamp, expr, get_json_object
from src.common.config_loader import load_config
from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("IngestKafkaWeatherToIceberg")


def run_kafka_to_iceberg_ingestion():
    config = load_config()
    
    kafka_bootstrap = config.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    kafka_topic = config.get("kafka", {}).get("topics", {}).get("weather", "weather_raw_events")
    checkpoint_location = config.get("iceberg", {}).get(
        "checkpoints", {}
    ).get("weather_bronze", "s3a://airline-data-lakehouse/checkpoints/bronze_weather_kafka")
    
    target_table = "glue_catalog.airline_bronze_db.raw_weather_kafka"

    logger.info("Starting Spark session for Kafka -> Iceberg Bronze ingestion...")
    spark = get_spark_session(app_name="IngestKafkaWeatherToIceberg")

    logger.info(f"Reading from Kafka topic '{kafka_topic}' at '{kafka_bootstrap}'...")
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Transform: parse airport_code from key or JSON payload before writing
    bronze_df = (
        raw_kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic")
        )
        .withColumn(
            "airport_code",
            coalesce(
                col("kafka_key"),
                get_json_object(col("raw_payload"), "$.location_code"),
                expr("'UNKNOWN'")
            )
        )
        .withColumn("ingested_at", current_timestamp())
        .select("airport_code", "raw_payload", "ingested_at", "kafka_topic")
    )

    logger.info(f"Writing streaming micro-batches to Iceberg table '{target_table}'...")
    query = (
        bronze_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .toTable(target_table)
    )

    query.awaitTermination()
    logger.info("Dynamic Bronze ingestion finished successfully.")


if __name__ == "__main__":
    run_kafka_to_iceberg_ingestion()