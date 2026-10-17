#script inspect actual silver transformation happening in aws
import sys
import logging
from pathlib import Path

# Add project root to sys.path to resolve src imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.spark import SedonaContext
from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("InspectSilverWeather")

TABLE_NAME = "silver_catalog.airline_silver_db.fact_airport_weather"


def inspect_schema(spark: SparkSession):
    print("\n================ 1. SILVER TABLE SCHEMA ================")
    spark.sql(f"DESCRIBE TABLE {TABLE_NAME}").show(truncate=False)


def inspect_row_counts(spark: SparkSession):
    print("\n================ 2. RECORD COUNT BY AIRPORT ================")
    spark.sql(f"""
        SELECT 
            airport_code, 
            COUNT(*) as record_count, 
            MIN(processed_at) as earliest_processed, 
            MAX(processed_at) as latest_processed 
        FROM {TABLE_NAME} 
        GROUP BY airport_code
    """).show(truncate=False)


def inspect_sample_data(spark: SparkSession, limit: int = 5):
    print("\n================ 3. SAMPLE FLATTENED METRICS & SEDONA GEOMETRY ================")
    df = spark.table(TABLE_NAME)
    (
        df.select(
            col("airport_code"),
            col("observation_time"),
            col("temperature_2m"),
            col("wind_speed_10m"),
            col("geometry")
        )
        .show(limit, truncate=False)
    )


def inspect_snapshot_history(spark: SparkSession):
    print("\n================ 4. ICEBERG SNAPSHOT COMMIT HISTORY ================")
    spark.sql(f"""
        SELECT committed_at, snapshot_id, operation, summary 
        FROM {TABLE_NAME}.snapshots
    """).show(truncate=False)


def main():
    logger.info("Initializing Sedona-enabled Spark session for Silver inspection...")
    spark = get_spark_session(app_name="InspectSilverWeather")
    SedonaContext.create(spark)

    inspect_schema(spark)
    inspect_row_counts(spark)
    inspect_sample_data(spark)
    inspect_snapshot_history(spark)


if __name__ == "__main__":
    main()