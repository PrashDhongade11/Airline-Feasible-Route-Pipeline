# Open Lakehouse Aviation Route Feasibility Pipeline

An end-to-end, event-driven Data Lakehouse pipeline that processes streaming weather metrics alongside static spatial aviation data to compute real-time flight route feasibility. Built using an open-source, cloud-native stack featuring **Apache Kafka, PySpark, Apache Sedona, Apache Iceberg, AWS S3, AWS Glue Data Catalog, and AWS Lambda/SNS**.

---

## 🏗️ Architecture Overview

The pipeline follows the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** to convert raw streaming weather payloads into clean, spatially enriched business aggregations stored as **Apache Iceberg** open table formats.

```
+-----------------------------------------------------------------------------------+
|                                  DATA SOURCES                                     |
|  Open-Meteo API | NOAA Aviation METAR | OurAirports Static Metadata               |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                              INGESTION LAYER (BRONZE)                             |
|  Kafka Producers -> Kafka Broker (`weather-raw-events`)                           |
|  -> Kafka S3 Consumer / Spark Structured Streaming                                |
|  -> AWS S3 Landing / Apache Iceberg (`raw_weather_kafka`)                         |
|  -> AWS Lambda (Triggered on S3 Event) -> AWS SNS Alert                           |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                            TRANSFORMATION LAYER (SILVER)                          |
|  PySpark ETL + Apache Sedona (`ST_Point` Geometries)                              |
|  Data Quality Checks & Dead Letter Queue (DLQ) Quarantining                       |
|  -> Iceberg Silver Table (`fact_airport_weather`)                                 |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                              ANALYTICS LAYER (GOLD)                               |
|  Route Feasibility Classification (`OPTIMAL`, `WARNING`, `RESTRICTED`)            |
|  -> Iceberg Gold Table (`route_feasibility`) -> AWS Athena Querying               |
+-----------------------------------------------------------------------------------+
```

---

## 🧰 Tech Stack

*   **Streaming & Messaging:** Apache Kafka (Dockerized)
*   **Processing Engine:** Apache Spark (PySpark), Apache Sedona (Geospatial extension)
*   **Table Format & Catalog:** Apache Iceberg, AWS Glue Data Catalog
*   **Cloud Storage & Alerts:** AWS S3, AWS Lambda, AWS SNS (Simple Notification Service)
*   **External Data APIs:** Open-Meteo Forecast API, NOAA Aviation Weather API (METAR), OurAirports Metadata
*   **Language & Query:** Python 3.12, SQL, GeoPandas / Shapely

---

## ☁️ AWS Cloud Infrastructure Integration

*   **AWS S3 (Simple Storage Service):** Serves as the primary object storage for isolated lakehouse buckets:
    *   `airline-lakehouse-bronze-prashant`: Stores raw JSON batches landed from Kafka and raw Iceberg tables.
    *   `airline-lakehouse-silver-prashant`: Stores processed Iceberg tables with Apache Sedona spatial geometries.
    *   `airline-lakehouse-gold-prashant`: Stores business-ready aggregated tables partitioned by `airport_code`.
*   **AWS Glue Data Catalog:** Functions as the centralized metastore across all Medallion tiers, keeping schema metadata synchronized for querying via external engines like Amazon Athena.
*   **AWS Lambda & AWS SNS:** 
    *   An S3 Event Notification triggers an **AWS Lambda function** whenever a new batch lands in the Bronze S3 bucket.
    *   The Lambda function validates object size and publishes an event alert to an **AWS SNS Topic** to notify downstream monitoring systems of fresh data arrival.

---

## 🌐 External APIs Used

1. **Open-Meteo Forecast API:** Provides real-time meteorological metrics including temperature, humidity, surface wind speeds (`10m`), cruising altitude wind speeds (`100m`), and weather codes.
2. **NOAA Aviation Weather API:** Fetches live METAR (Meteorological Aerodrome Report) observations for international airport codes (e.g., Delhi/VIDP).
3. **OurAirports Data Repository:** Static spatial data source providing global airport coordinates, IATA codes, and country classifications.

---

## 📜 Script Reference Guide

| Script File | Layer / Purpose | Functionality Description |
| :--- | :--- | :--- |
| `kafka_producer.py` | Ingestion (Producer) | Continuously fetches live weather telemetry from Open-Meteo API and publishes JSON payloads to the Kafka topic. |
| `kafka_producer_sim.py` | Ingestion (Producer) | Simulates multi-airport streaming events (BOM, DEL, BLR, PNQ) by fetching live weather and pushing events into Kafka. |
| `kafka_s3_consumer.py` | Ingestion (Sink) | Consumes streaming events from Kafka in batches and directly flushes raw JSON files into the S3 Bronze bucket. |
| `ingest_kafka_weather_to_iceberg.py` | Ingestion (Bronze) | PySpark Structured Streaming job that reads from Kafka and writes directly to an Apache Iceberg Bronze table. |
| `backfill_bronze_null_airports.py` | Data Quality (Bronze) | PySpark maintenance script that uses Iceberg `UPDATE` queries to repair missing `airport_code` fields using JSON payload extraction. |
| `weather_silver_transformation.py` | Transformation (Silver) | Parses raw telemetry, enforces bounds checks, routes malformed records to a Dead Letter Queue (DLQ), and constructs Sedona `ST_Point` spatial geometries. |
| `silver_to_gold.py` | Aggregation (Gold) | Evaluates flight safety metrics (wind speed, weather codes) and materializes route feasibility classifications (`OPTIMAL`, `WARNING`, `RESTRICTED`) into a partitioned Iceberg table. |
| `generate_route_buffers.py` | Spatial Validation | Utility script that extracts airport pairs, constructs `LineString` flight paths using Shapely, creates 15km spatial buffer polygons, and tests API connectivity. |

---

## 🚀 Step-by-Step Execution Guide

### 1. Start Infrastructure
Ensure Docker is running and spin up the local Kafka broker:
```bash
docker compose -f docker/docker-compose.yml up -d
```

### 2. Run Ingestion Stream
In Terminal 1 (Producer):
```bash
python scripts/kafka_producer.py
```
In Terminal 2 (Consumer):
```bash
python scripts/kafka_s3_consumer.py
```

### 3. Run Silver Transformation
Process raw Bronze records into cleaned Iceberg tables with Sedona geometries:
```bash
python src/transformations/weather_silver_transformation.py
```

### 4. Run Gold Feasibility Aggregations
Compute route feasibility statuses for business consumption:
```bash
python src/transformations/silver_to_gold.py
```

---

## 💡 Architectural & Engineering Decisions

*   **Decoupled Orchestration:** Rather than running resource-heavy managed orchestration services (e.g., AWS Glue) in a local sandbox, the pipeline is designed modularly using standalone Python and PySpark modules. This maintains zero cloud compute costs during development while keeping the pipeline fully ready for enterprise Airflow deployment.
*   **Data Lakehouse Isolation:** Storage and compute are strictly separated across dedicated S3 buckets per Medallion tier, managed via AWS Glue Data Catalog and Apache Iceberg for ACID transactions, schema evolution, and time travel.