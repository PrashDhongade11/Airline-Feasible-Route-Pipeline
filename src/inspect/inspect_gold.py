import sys
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.spark import SedonaContext
from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("InspectGoldPipeline")

GOLD_TABLE = "gold_catalog.airline_gold_db.route_feasibility"


def inspect_schema(spark: SparkSession):
    print("\n================ 1. GOLD TABLE SCHEMA ================")
    spark.sql(f"DESCRIBE TABLE {GOLD_TABLE}").show(truncate=False)


def inspect_feasibility_summary(spark: SparkSession):
    print("\n================ 2. RECORD COUNT BY FEASIBILITY STATUS ================")
    spark.sql(f"""
        SELECT 
            feasibility_status, 
            COUNT(*) as airport_count,
            MIN(evaluated_at) as earliest_evaluation,
            MAX(evaluated_at) as latest_evaluation
        FROM {GOLD_TABLE}
        GROUP BY feasibility_status
    """).show(truncate=False)


def inspect_sample_data(spark: SparkSession, limit: int = 10):
    print("\n================ 3. SAMPLE AIRPORT ROUTE FEASIBILITY RECORDS ================")
    df = spark.table(GOLD_TABLE)
    (
        df.select(
            col("airport_code"),
            col("latitude"),
            col("longitude"),
            col("temperature_2m"),
            col("wind_speed_10m"),
            col("weather_code"),
            col("feasibility_status"),
            col("evaluated_at")
        )
        .show(limit, truncate=False)
    )


def inspect_snapshot_history(spark: SparkSession):
    print("\n================ 4. ICEBERG SNAPSHOT COMMIT HISTORY ================")
    spark.sql(f"""
        SELECT committed_at, snapshot_id, operation, summary 
        FROM {GOLD_TABLE}.snapshots
    """).show(truncate=False)


def main():
    logger.info("Initializing Sedona-enabled Spark session for Gold inspection...")
    spark = get_spark_session(app_name="InspectGoldPipeline")
    SedonaContext.create(spark)

    inspect_schema(spark)
    inspect_feasibility_summary(spark)
    inspect_sample_data(spark)
    inspect_snapshot_history(spark)


if __name__ == "__main__":
    main()