import sys
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, lit
from sedona.spark import SedonaContext
from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SilverToGoldFeasibility")

# Multi-catalog references bound to isolated S3 storage
SILVER_WEATHER_TABLE = "silver_catalog.airline_silver_db.fact_airport_weather"
GOLD_DB = "airline_gold_db"
GOLD_TABLE = "route_feasibility"
FULL_GOLD_TABLE = f"gold_catalog.{GOLD_DB}.{GOLD_TABLE}"


def ensure_gold_table_exists(spark: SparkSession):
    logger.info(f"Ensuring database `{GOLD_DB}` exists under `gold_catalog`...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_catalog.{GOLD_DB}")

    logger.info(f"Ensuring Iceberg table `{FULL_GOLD_TABLE}` exists in dedicated Gold S3 bucket...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FULL_GOLD_TABLE} (
        airport_code STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        temperature_2m DOUBLE,
        wind_speed_10m DOUBLE,
        weather_code INT,
        feasibility_status STRING,
        evaluated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (airport_code)
    """
    spark.sql(create_table_sql)
    logger.info(f"Gold table verified under `airline-lakehouse-gold-prashant`.")


def run_gold_transformation():
    logger.info("Initializing Sedona-enabled Spark session for Gold transformation...")
    spark = get_spark_session(app_name="SilverToGoldFeasibility")
    SedonaContext.create(spark)

    ensure_gold_table_exists(spark)

    logger.info(f"Reading Silver weather data from: {SILVER_WEATHER_TABLE}")
    weather_df = spark.table(SILVER_WEATHER_TABLE)

    # Step 1: Evaluate route feasibility based on weather telemetry
    feasibility_df = (
        weather_df
        .select(
            col("airport_code"),
            col("latitude"),
            col("longitude"),
            col("temperature_2m"),
            col("wind_speed_10m"),
            col("weather_code")
        )
        .withColumn(
            "feasibility_status",
            when((col("wind_speed_10m").isNotNull()) & (col("wind_speed_10m") > 40.0), lit("RESTRICTED"))
            .when((col("weather_code").isNotNull()) & (col("weather_code") >= 50), lit("WARNING"))
            .otherwise(lit("OPTIMAL"))
        )
        .withColumn("evaluated_at", current_timestamp())
    )

    logger.info(f"Writing Gold feasibility aggregations to Iceberg Gold table: {FULL_GOLD_TABLE}")
    feasibility_df.write \
        .format("iceberg") \
        .mode("append") \
        .save(FULL_GOLD_TABLE)

    logger.info("Gold route feasibility transformation completed successfully!")


if __name__ == "__main__":
    run_gold_transformation()