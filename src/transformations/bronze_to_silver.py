import sys
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from sedona.spark import SedonaContext
from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WeatherSilverTransformation")

# Multi-catalog references bound to isolated S3 buckets
BRONZE_TABLE = "bronze_catalog.airline_bronze_db.raw_weather_kafka"
SILVER_DB = "airline_silver_db"
SILVER_TABLE = "fact_airport_weather"
FULL_SILVER_TABLE = f"silver_catalog.{SILVER_DB}.{SILVER_TABLE}"

# Explicit schema matching Open-Meteo payload structure stored in raw_payload
OPEN_METEO_PAYLOAD_SCHEMA = StructType([
    StructField("location_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("current", StructType([
        StructField("time", StringType(), True),
        StructField("interval", IntegerType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("relative_humidity_2m", IntegerType(), True),
        StructField("apparent_temperature", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("rain", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("cloud_cover", IntegerType(), True),
        StructField("pressure_msl", DoubleType(), True),
        StructField("surface_pressure", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
        StructField("wind_direction_10m", IntegerType(), True),
        StructField("wind_gusts_10m", DoubleType(), True)
    ]), True)
])


def ensure_silver_table_exists(spark: SparkSession):
    logger.info(f"Ensuring database `{SILVER_DB}` exists under `silver_catalog`...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver_catalog.{SILVER_DB}")

    logger.info(f"Ensuring Iceberg table `{FULL_SILVER_TABLE}` exists in dedicated Silver S3 bucket...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FULL_SILVER_TABLE} (
        airport_code STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        elevation DOUBLE,
        timezone STRING,
        observation_time STRING,
        temperature_2m DOUBLE,
        relative_humidity_2m INT,
        apparent_temperature DOUBLE,
        precipitation DOUBLE,
        rain DOUBLE,
        weather_code INT,
        cloud_cover INT,
        pressure_msl DOUBLE,
        surface_pressure DOUBLE,
        wind_speed_10m DOUBLE,
        wind_direction_10m INT,
        wind_gusts_10m DOUBLE,
        geometry STRING,
        processed_at TIMESTAMP,
        kafka_topic STRING
    )
    USING iceberg
    PARTITIONED BY (airport_code)
    """
    spark.sql(create_table_sql)
    logger.info(f"Silver table verified under `airline-lakehouse-silver-prashant`.")


def run_transformation():
    logger.info("Initializing Sedona-enabled Spark session for Silver transformation...")
    spark = get_spark_session(app_name="WeatherSilverTransformation")
    
    SedonaContext.create(spark)

    ensure_silver_table_exists(spark)

    logger.info(f"Reading raw data from Bronze table: {BRONZE_TABLE}")
    bronze_df = spark.table(BRONZE_TABLE)

    # Step 1: Parse JSON payload against explicit schema and flatten fields
    parsed_df = (
        bronze_df
        .withColumn("parsed_json", from_json(col("raw_payload"), OPEN_METEO_PAYLOAD_SCHEMA))
        .select(
            col("airport_code"),
            col("kafka_topic"),
            col("parsed_json.*")
        )
    )

    # Step 2: Unpack nested 'current' structure and build Sedona ST_Point geometries
    flattened_df = (
        parsed_df
        .select(
            col("airport_code"),
            col("latitude"),
            col("longitude"),
            col("elevation"),
            col("timezone"),
            col("current.time").alias("observation_time"),
            col("current.temperature_2m").alias("temperature_2m"),
            col("current.relative_humidity_2m").alias("relative_humidity_2m"),
            col("current.apparent_temperature").alias("apparent_temperature"),
            col("current.precipitation").alias("precipitation"),
            col("current.rain").alias("rain"),
            col("current.weather_code").alias("weather_code"),
            col("current.cloud_cover").alias("cloud_cover"),
            col("current.pressure_msl").alias("pressure_msl"),
            col("current.surface_pressure").alias("surface_pressure"),
            col("current.wind_speed_10m").alias("wind_speed_10m"),
            col("current.wind_direction_10m").alias("wind_direction_10m"),
            col("current.wind_gusts_10m").alias("wind_gusts_10m"),
            col("kafka_topic")
        )
        .filter(col("airport_code").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())
    )

    # Step 3: Add Apache Sedona geometry and deduplicate records
    spatial_df = (
        flattened_df
        .withColumn("geom", expr("ST_Point(CAST(longitude AS Decimal(24,10)), CAST(latitude AS Decimal(24,10)))"))
        .withColumn("geometry", expr("ST_AsText(geom)"))
        .drop("geom")
        .withColumn("processed_at", current_timestamp())
        .dropDuplicates(["airport_code", "observation_time"])
    )

    logger.info(f"Writing cleaned and spatialized records into Iceberg Silver table: {FULL_SILVER_TABLE}")
    spatial_df.write \
        .format("iceberg") \
        .mode("append") \
        .save(FULL_SILVER_TABLE)

    logger.info("Silver weather transformation completed successfully!")


if __name__ == "__main__":
    run_transformation()