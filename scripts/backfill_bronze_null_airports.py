import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BackfillBronzeAirports")

TABLE_NAME = "glue_catalog.airline_bronze_db.raw_weather_kafka"


def backfill_null_airports():
    spark = get_spark_session(app_name="BackfillBronzeAirports")
    
    logger.info(f"Checking for NULL airport_code records in {TABLE_NAME}...")
    null_count = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE airport_code IS NULL").collect()[0][0]
    
    if null_count == 0:
        logger.info("No NULL airport_code records found. No backfill needed.")
        return

    logger.info(f"Found {null_count} records with NULL airport_code. Executing Iceberg UPDATE...")
    
    spark.sql(f"""
        UPDATE {TABLE_NAME}
        SET airport_code = COALESCE(get_json_object(raw_payload, '$.location_code'), 'UNKNOWN')
        WHERE airport_code IS NULL
    """)

    logger.info("Backfill completed successfully!")


if __name__ == "__main__":
    backfill_null_airports()