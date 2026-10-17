import json
import time
import requests
from kafka import KafkaProducer

# Open-Meteo API endpoint for Pune coordinates (or active flight coordinates)
LATITUDE = 18.5204
LONGITUDE = 73.8567
API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(7, 4, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'weather-raw-events'

def fetch_and_produce():
    try:
        response = requests.get(API_URL, timeout=10)
        if response.status_code == 200:
            payload = response.json()
            payload['ingest_timestamp'] = time.time()
            producer.send(TOPIC_NAME, value=payload)
            producer.flush()
            print(f"[PRODUCER] Sent weather event: {payload['current']}")
        else:
            print(f"[PRODUCER] API Error: {response.status_code}")
    except Exception as e:
        print(f"[PRODUCER] Error fetching data: {e}")

if __name__ == '__main__':
    print("Starting Weather Kafka Producer... Press Ctrl+C to stop.")
    while True:
        fetch_and_produce()
        time.sleep(10)  # Produce every 10 seconds