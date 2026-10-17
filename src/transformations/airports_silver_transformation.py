"""
Production-Grade Silver Layer Transformation for Airports Dataset.
Implements DLQ pattern, strict coordinate validation, IATA/ICAO regex checks,
and Apache Sedona ST_Point creation.
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, trim, upper, when, regexp_extract, coalesce, lit
)
from src.common.logger import get_logger

logger = get_logger("AirportsSilverTransformation")

# RegEx patterns for standard airport codes
IATA_REGEX = "^[A-Z0-9]{3}$"
ICAO_REGEX = "^[A-Z]{4}$"


def transform_airports_silver(
    spark: SparkSession, raw_airports_df: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Transforms raw airport data into Silver tier.

    Returns:
        Tuple[DataFrame, DataFrame]: (valid_silver_df, quarantined_dlq_df)
    """
    logger.info("Starting production Silver transformation for Airports dataset...")

    if raw_airports_df.isEmpty():
        logger.warning("Input raw_airports_df is empty. Returning empty DataFrames.")
        return raw_airports_df, raw_airports_df

    # 1. Clean basic string attributes
    cleaned_df = (
        raw_airports_df
        .withColumn("iata_code", upper(trim(col("iata_code"))))
        .withColumn("icao_code", upper(trim(col("icao_code"))))
        .withColumn("airport_name", trim(col("name")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
    )

    # 2. Apply Data Quality Rules & Flag Violations
    validated_df = cleaned_df.withColumn(
        "is_invalid",
        when(col("iata_code").isNull() | (regexp_extract(col("iata_code"), IATA_REGEX, 0) == ""), lit("INVALID_IATA"))
        .when(col("latitude").isNull() | (col("latitude") < -90.0) | (col("latitude") > 90.0), lit("INVALID_LATITUDE"))
        .when(col("longitude").isNull() | (col("longitude") < -180.0) | (col("longitude") > 180.0), lit("INVALID_LONGITUDE"))
        .when((col("latitude") == 0.0) & (col("longitude") == 0.0), lit("NULL_ISLAND_COORDINATES"))
        .otherwise(lit(None))
    )

    # 3. Separate Quarantined Records (DLQ) vs Valid Records
    quarantined_df = validated_df.filter(col("is_invalid").isNotNull())
    valid_records_df = validated_df.filter(col("is_invalid").isNull())

    # 4. Construct Sedona Geometry on Valid Dataset
    sedona_df = valid_records_df.withColumn(
        "airport_geom",
        expr("ST_Point(longitude, latitude)")
    )

    silver_df = sedona_df.select(
        "iata_code",
        "icao_code",
        "airport_name",
        "city",
        "state",
        "latitude",
        "longitude",
        "airport_geom"
    )

    valid_cnt = silver_df.count()
    dlq_cnt = quarantined_df.count()
    logger.info(f"Airports Transformation complete. Valid records: {valid_cnt}, Quarantined (DLQ): {dlq_cnt}")

    return silver_df, quarantined_df


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("AirportsSilverTest")

    # Sample includes valid, invalid coordinates, and bad IATA codes
    sample_data = [
        ("DEL", "VIDP", "Indira Gandhi Intl", "New Delhi", "Delhi", "28.5562", "77.1000"),
        ("BOM", "VABB", "Chhatrapati Shivaji Intl", "Mumbai", "Maharashtra", "19.0896", "72.8656"),
        ("INVALID123", "XXXX", "Bad Airport", "Nowhere", "NA", "0.0", "0.0"),  # Bad IATA & Null Island
        ("BLR", "VOBL", "Kempegowda Intl", "Bengaluru", "Karnataka", "999.0", "77.7063"),  # Bad Latitude
    ]
    schema = ["iata_code", "icao_code", "name", "city", "state", "latitude", "longitude"]
    raw_df = spark.createDataFrame(sample_data, schema=schema)

    valid_df, dlq_df = transform_airports_silver(spark, raw_df)

    print("=== VALID SILVER AIRPORTS ===")
    valid_df.show(truncate=False)

    print("=== QUARANTINED (DLQ) AIRPORTS ===")
    dlq_df.select("iata_code", "latitude", "longitude", "is_invalid").show(truncate=False)

    spark.stop()