"""
Centralized SparkSession factory with Apache Sedona geospatial integration,
Apache Iceberg multi-catalog support, Kafka streaming, and AWS Glue Data Catalog integration.
"""
import os
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def get_spark_session(app_name: str = "AirlineFeasibleRoutePipeline") -> SparkSession:
    java_module_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
        "org.apache.sedona:sedona-spark-3.5_2.12:1.6.1",
        "org.datasyslab:geotools-wrapper:1.6.1-28.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.261",
    ]

    builder = (
        SedonaContext.builder()
        .appName(app_name)
        .config("spark.driver.extraJavaOptions", java_module_options)
        .config("spark.executor.extraJavaOptions", java_module_options)
        .config("spark.jars.packages", ",".join(packages))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.viz.sql.SedonaVizExtensions",
        )
        # --- BRONZE CATALOG ---
        .config("spark.sql.catalog.bronze_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bronze_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.bronze_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.bronze_catalog.warehouse", "s3://airline-lakehouse-bronze-prashant/iceberg-warehouse/")
        
        # --- SILVER CATALOG ---
        .config("spark.sql.catalog.silver_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.silver_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.silver_catalog.warehouse", "s3://airline-lakehouse-silver-prashant/iceberg-warehouse/")

        # --- GOLD CATALOG ---
        .config("spark.sql.catalog.gold_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.gold_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.gold_catalog.warehouse", "s3://airline-lakehouse-gold-prashant/iceberg-warehouse/")

        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .master("local[*]")
    )

    spark = builder.getOrCreate()
    spark = SedonaContext.create(spark)

    return spark


if __name__ == "__main__":
    print("Testing PySpark + Sedona + Iceberg + Multi-Catalog AWS Glue initialization...")
    spark = get_spark_session("TestLakehouseSession")

    print("\n--- Testing Sedona Spatial Query ---")
    test_df = spark.sql(
        "SELECT ST_Buffer(ST_Point(73.9197, 18.5821), 0.15) AS sample_buffer_geom"
    )
    test_df.show(truncate=False)

    print("\n--- Testing Bronze Catalog Connectivity ---")
    spark.sql("SHOW DATABASES IN bronze_catalog").show()

    print("\n--- Testing Silver Catalog Connectivity ---")
    spark.sql("SHOW DATABASES IN silver_catalog").show()

    spark.stop()