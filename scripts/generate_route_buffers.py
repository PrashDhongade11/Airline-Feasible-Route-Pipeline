import json
import logging
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# -------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("Phase1Validation")

logger.info("=" * 60)
logger.info("PHASE 1: GEOSPATIAL DATA SOURCE EXPLORATION & VALIDATION")
logger.info("=" * 60)

# -------------------------------------------------------------------
# SOURCE 1: OurAirports (Static CSV) - Indian Airports Extraction
# -------------------------------------------------------------------
logger.info("\n[1/3] Testing Source 1: OurAirports (Static Airport Metadata)...")
OURAIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

DEFAULT_PNQ_COORDS = (73.9197, 18.5822)
DEFAULT_DEL_COORDS = (77.1000, 28.5562)

route_buffer = None

try:
    # Removed invalid timeout parameter
    df_airports = pd.read_csv(OURAIRPORTS_URL)
    indian_airports = df_airports[
        (df_airports['iso_country'] == 'IN') & 
        (df_airports['type'].isin(['large_airport', 'medium_airport']))
    ][['iata_code', 'name', 'municipality', 'latitude_deg', 'longitude_deg']].dropna()

    logger.info(f"Successfully fetched {len(indian_airports)} Indian airports.")

    pnq_match = indian_airports[indian_airports['iata_code'] == 'PNQ']
    del_match = indian_airports[indian_airports['iata_code'] == 'DEL']

    if not pnq_match.empty and not del_match.empty:
        pune = pnq_match.iloc[0]
        delhi = del_match.iloc[0]
        pune_point = Point(pune['longitude_deg'], pune['latitude_deg'])
        delhi_point = Point(delhi['longitude_deg'], delhi['latitude_deg'])
    else:
        logger.warning("PNQ or DEL missing in source CSV. Falling back to default static coordinates.")
        pune_point = Point(*DEFAULT_PNQ_COORDS)
        delhi_point = Point(*DEFAULT_DEL_COORDS)

    route_line = LineString([pune_point, delhi_point])
    route_buffer = route_line.buffer(0.15)
    
    logger.info(f"Flight LineString PNQ -> DEL: {route_line}")
    logger.info(f"15km Route Buffer Polygon bounds: {route_buffer.bounds}")

except Exception as e:
    logger.error(f"Source 1 Execution Error: {e}. Generating buffer using fallback coordinates.")
    pune_point = Point(*DEFAULT_PNQ_COORDS)
    delhi_point = Point(*DEFAULT_DEL_COORDS)
    route_line = LineString([pune_point, delhi_point])
    route_buffer = route_line.buffer(0.15)

# -------------------------------------------------------------------
# SOURCE 2: Live Weather APIs (Open-Meteo & NOAA METAR)
# -------------------------------------------------------------------
logger.info("\n[2/3] Testing Source 2: Live Weather APIs...")

# 2A. Open-Meteo Hourly Forecast along Pune Coordinates
open_meteo_url = (
    f"https://api.open-meteo.com/v1/forecast?"
    f"latitude={pune_point.y}&longitude={pune_point.x}"
    f"&current_weather=true"
)
try:
    res = requests.get(open_meteo_url, timeout=5)
    res.raise_for_status()
    payload = res.json()
    
    weather_data = payload.get('current_weather')
    if weather_data:
        pune_weather_point = Point(pune_point.x, pune_point.y)
        is_intersecting = route_buffer.intersects(pune_weather_point) if route_buffer else False
        temp = weather_data.get('temperature', 'N/A')
        wind = weather_data.get('windspeed', 'N/A')
        logger.info(f"Open-Meteo (Pune): Temp {temp}°C, Wind {wind} km/h")
        logger.info(f"Spatial Intersects Check (Weather Point in Route Buffer): {is_intersecting}")
    else:
        logger.warning("Open-Meteo response missing 'current_weather' key.")

except requests.exceptions.RequestException as e:
    logger.error(f"Open-Meteo API connection failed: {e}")

# 2B. NOAA Aviation Weather API (METAR format for DEL)
noaa_metar_url = "https://aviationweather.gov/api/data/metar?ids=VIDP&format=json"
try:
    res_noaa = requests.get(noaa_metar_url, timeout=5)
    if res_noaa.status_code == 200 and res_noaa.text.strip():
        data = res_noaa.json()
        if isinstance(data, list) and len(data) > 0:
            metar_raw = data[0].get('rawOb', 'No raw observation field')
            logger.info(f"NOAA Aviation METAR (Delhi/VIDP): {metar_raw}")
        else:
            logger.info("NOAA METAR returned empty list payload.")
    else:
        logger.info(f"NOAA METAR response status {res_noaa.status_code} (Off-peak window or no observation).")
except Exception as e:
    logger.error(f"NOAA METAR API Request Failed: {e}")

# -------------------------------------------------------------------
# SOURCE 3: AWS Open Data (Public NOAA S3 Bucket Access without credentials)
# -------------------------------------------------------------------
logger.info("\n[3/3] Testing Source 3: AWS Open Data (NOAA GFS S3 Bucket)...")

# Configure Boto3 client with unsigned access and explicit timeouts
s3_config = Config(
    signature_version=UNSIGNED,
    connect_timeout=5,
    read_timeout=5,
    retries={'max_attempts': 2}
)

NOAA_BUCKET = "noaa-gfs-bdp-pds"

try:
    s3_client = boto3.client('s3', config=s3_config)
    response = s3_client.list_objects_v2(Bucket=NOAA_BUCKET, MaxKeys=5)
    
    if 'Contents' in response:
        logger.info(f"Successfully accessed AWS Open Data S3 Bucket: '{NOAA_BUCKET}'")
        for obj in response['Contents']:
            logger.info(f"   - {obj['Key']} ({obj['Size']} bytes)")
    else:
        logger.warning("S3 Bucket reachable, but returned 0 objects.")

except (BotoCoreError, ClientError) as e:
    logger.error(f"AWS Open Data S3 Access Failed: {e}")

logger.info("\n" + "=" * 60)
logger.info("DATA FETCHING VALIDATION COMPLETE - READY FOR PHASE 2 (SPARK ETL)")
logger.info("=" * 60)