"""
Production-Grade Silver Layer Transformation for Flight Routes Dataset.
Features strict timestamp parsing against explicit date patterns, IATA checks,
sequence logic (arrival > departure), and DLQ segregation.
"""
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, coalesce, when, lit, regexp_extract
)
from src.common.logger import get_logger

logger = get_logger("RouteSilverTransformation")

IATA_REGEX = "^[A-Z0-9]{3}$"
# Allowed timestamp formats from upstream sources
TIMESTAMP_FORMATS: List[str] = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm"
]


def transform_route_silver(
    spark: SparkSession, raw_route_df: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Transforms raw flight route data with robust parsing and validation.

    Returns:
        Tuple[DataFrame, DataFrame]: (valid_silver_df, quarantined_dlq_df)
    """
    logger.info("Starting production Silver transformation for Flight Routes dataset...")

    if raw_route_df.isEmpty():
        logger.warning("Input raw_route_df is empty. Returning empty DataFrames.")
        return raw_route_df, raw_route_df

    # 1. Clean airport string fields
    cleaned_df = (
        raw_route_df
        .withColumn("src_airport_iata", upper(trim(col("src_airport_iata"))))
        .withColumn("dst_airport_iata", upper(trim(col("dst_airport_iata"))))
        .withColumn("src_airport_icao", upper(trim(col("src_airport_icao"))))
        .withColumn("dst_airport_icao", upper(trim(col("dst_airport_icao"))))
    )

    # 2. Strict multi-format timestamp parsing
    dep_parsed = [to_timestamp(col("departure_time"), fmt) for fmt in TIMESTAMP_FORMATS]
    arr_parsed = [to_timestamp(col("arrival_time"), fmt) for fmt in TIMESTAMP_FORMATS]

    parsed_df = (
        cleaned_df
        .withColumn("departure_timestamp", coalesce(*dep_parsed))
        .withColumn("arrival_timestamp", coalesce(*arr_parsed))
    )

    # 3. Data Quality & Logical Constraints
    validated_df = parsed_df.withColumn(
        "is_invalid",
        when(col("src_airport_iata").isNull() | (regexp_extract(col("src_airport_iata"), IATA_REGEX, 0) == ""), lit("INVALID_SRC_IATA"))
        .when(col("dst_airport_iata").isNull() | (regexp_extract(col("dst_airport_iata"), IATA_REGEX, 0) == ""), lit("INVALID_DST_IATA"))
        .when(col("src_airport_iata") == col("dst_airport_iata"), lit("SAME_SRC_AND_DST"))
        .when(col("departure_timestamp").isNull(), lit("MALFORMED_DEP_TIMESTAMP"))
        .when(col("arrival_timestamp").isNotNull() & (col("arrival_timestamp") <= col("departure_timestamp")), lit("ARR_BEFORE_DEP"))
        .otherwise(lit(None))
    )

    # 4. DLQ Splitting
    quarantined_df = validated_df.filter(col("is_invalid").isNotNull())
    valid_df = validated_df.filter(col("is_invalid").isNull())

    result_df = valid_df.select(
        "src_airport_iata",
        "dst_airport_iata",
        "src_airport_icao",
        "dst_airport_icao",
        "departure_timestamp",
        "arrival_timestamp"
    )

    valid_cnt = result_df.count()
    dlq_cnt = quarantined_df.count()
    logger.info(f"Route Transformation complete. Valid records: {valid_cnt}, Quarantined (DLQ): {dlq_cnt}")

    return result_df, quarantined_df


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("RouteSilverTest")

    sample_routes = [
        ("DEL", "BOM", "VIDP", "VABB", "2026-09-26 10:00:00", "2026-09-26 12:15:00"),  # Valid
        ("DEL", "DEL", "VIDP", "VIDP", "2026-09-26 10:00:00", "2026-09-26 12:15:00"),  # Same src & dst
        ("BLR", "HYD", "VOBL", "VOHS", "INVALID_DATE_STRING", "2026-09-26 12:15:00"),  # Bad timestamp
        ("MAA", "CCU", "VOMM", "VECC", "2026-09-26 14:00:00", "2026-09-26 11:00:00"),  # Arr before dep
    ]
    schema = ["src_airport_iata", "dst_airport_iata", "src_airport_icao", "dst_airport_icao", "departure_time", "arrival_time"]
    raw_df = spark.createDataFrame(sample_routes, schema=schema)

    valid_df, dlq_df = transform_route_silver(spark, raw_df)

    print("=== VALID SILVER ROUTES ===")
    valid_df.show(truncate=False)

    print("=== QUARANTINED (DLQ) ROUTES ===")
    dlq_df.select("src_airport_iata", "dst_airport_iata", "departure_time", "is_invalid").show(truncate=False)

    spark.stop()