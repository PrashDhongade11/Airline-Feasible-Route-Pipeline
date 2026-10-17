FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    libgeos-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    geopandas \
    shapely \
    pyiceberg \
    pandas \
    numpy \
    boto3 \
    kafka-python-ng \
    pyspark \
    apache-sedona