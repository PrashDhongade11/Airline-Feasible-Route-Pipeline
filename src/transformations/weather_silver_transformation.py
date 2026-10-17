"""
Production-Grade Silver Layer Transformation for Open-Meteo Weather Dataset.
Handles strict timestamp parsing, bounds validation on meteorological units,
unit conversion (km/h -> Knots, m -> km), and Sedona point generation.
"""
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, to_timestamp, coalesce, lit, when
)
from src.common.logger import get_logger


logger = get_logger("WeatherSilverTransformation")

# Conversion Constant
KMH_TO_KNOTS = 0.539957

TIMESTAMP_FORMATS: List[str] = [
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss"
]


def transform_weather_silver(
    spark: SparkSession, raw_weather_df: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Transforms raw weather observations into Silver tier.

    Returns:
        Tuple[DataFrame, DataFrame]: (valid_silver_df, quarantined_dlq_df)
    """
    logger.info("Starting production Silver transformation for Weather dataset...")

    if raw_weather_df.isEmpty():
        logger.warning("Input raw_weather_df is empty. Returning empty DataFrames.")
        return raw_weather_df, raw_weather_df

    # 1. Type casting and timestamp parsing
    ts_parsed = [to_timestamp(col("forecast_time"), fmt) for fmt in TIMESTAMP_FORMATS]

    cast_df = (
        raw_weather_df
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("weather_timestamp", coalesce(*ts_parsed))
        .withColumn("wind_speed_10m", col("wind_speed_10m").cast("double"))
        .withColumn("visibility", col("visibility").cast("double"))
        .withColumn("wind_speed_100m", col("wind_speed_100m").cast("double"))
        .withColumn("icing_index", col("icing_index").cast("double"))
        .withColumn("turbulence_index", col("turbulence_index").cast("double"))
        .withColumn("weather_code", col("weather_code").cast("int"))
    )

    # 2. Metric Transformations & Safe Default Estimations
    converted_df = (
        cast_df
        .withColumn("surface_wind_speed_knots", (col("wind_speed_10m") * lit(KMH_TO_KNOTS)))
        .withColumn("visibility_km", (col("visibility") / lit(1000.0)))
        .withColumn(
            "cruising_wind_speed_knots",
            coalesce(
                col("wind_speed_100m") * lit(KMH_TO_KNOTS),
                col("wind_speed_10m") * lit(KMH_TO_KNOTS) * lit(1.5)
            )
        )
        .withColumn("icing_index", coalesce(col("icing_index"), lit(0.0)))
        .withColumn("turbulence_index", coalesce(col("turbulence_index"), lit(0.0)))
        .withColumn("weather_code", coalesce(col("weather_code"), lit(0)))
    )

    # 3. Data Quality Bounds Checks
    validated_df = converted_df.withColumn(
        "is_invalid",
        when(col("latitude").isNull() | (col("latitude") < -90.0) | (col("latitude") > 90.0), lit("INVALID_LATITUDE"))
        .when(col("longitude").isNull() | (col("longitude") < -180.0) | (col("longitude") > 180.0), lit("INVALID_LONGITUDE"))
        .when(col("weather_timestamp").isNull(), lit("MALFORMED_WEATHER_TIMESTAMP"))
        .when(col("surface_wind_speed_knots") < 0.0, lit("NEGATIVE_WIND_SPEED"))
        .when(col("visibility_km") < 0.0, lit("NEGATIVE_VISIBILITY"))
        .otherwise(lit(None))
    )

    # 4. DLQ Splitting
    quarantined_df = validated_df.filter(col("is_invalid").isNotNull())
    valid_records_df = validated_df.filter(col("is_invalid").isNull())

    # 5. Sedona ST_Point Geometry
    sedona_df = valid_records_df.withColumn(
        "weather_geom",
        expr("ST_Point(longitude, latitude)")
    )

    result_df = sedona_df.select(
        "latitude",
        "longitude",
        "weather_timestamp",
        "surface_wind_speed_knots",
        "visibility_km",
        "cruising_wind_speed_knots",
        "icing_index",
        "turbulence_index",
        "weather_code",
        "weather_geom"
    )

    valid_cnt = result_df.count()
    dlq_cnt = quarantined_df.count()
    logger.info(f"Weather Transformation complete. Valid records: {valid_cnt}, Quarantined (DLQ): {dlq_cnt}")

    return result_df, quarantined_df


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("WeatherSilverTest")

    sample_weather = [
        (28.5562, 77.1000, "2026-09-26T10:00", 25.0, 10000.0, 45.0, 0.1, 0.2, 0),  # Valid ISO
        (19.0896, 72.8656, "2026-09-26 12:00:00", -10.0, 8000.0, 30.0, 0.0, 0.1, 0),  # Negative wind speed
        (999.0, 77.1000, "2026-09-26T10:00", 25.0, 10000.0, 45.0, 0.1, 0.2, 0),  # Invalid Lat
    ]
    schema = [
        "latitude", "longitude", "forecast_time", "wind_speed_10m",
        "visibility", "wind_speed_100m", "icing_index", "turbulence_index", "weather_code"
    ]
    raw_df = spark.createDataFrame(sample_weather, schema=schema)

    valid_df, dlq_df = transform_weather_silver(spark, raw_df)

    print("=== VALID SILVER WEATHER ===")
    valid_df.show(truncate=False)

    print("=== QUARANTINED (DLQ) WEATHER ===")
    dlq_df.select("latitude", "longitude", "forecast_time", "is_invalid").show(truncate=False)

    spark.stop()