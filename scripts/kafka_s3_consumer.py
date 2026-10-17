import json
import time
import boto3
from kafka import KafkaConsumer

BRONZE_BUCKET = "airline-lakehouse-bronze-prashant"
AWS_REGION = "us-east-1"
TOPIC_NAME = "weather-raw-events"
BATCH_SIZE = 5  # Flush batch to S3 every 5 events

s3_client = boto3.client('s3', region_name=AWS_REGION)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='s3-bronze-ingestion-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def upload_batch_to_s3(batch):
    timestamp_ms = int(time.time() * 1000)
    s3_key = f"landing/weather_batch_{timestamp_ms}.json"
    
    file_content = json.dumps(batch, indent=2)
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=file_content,
        ContentType='application/json'
    )
    print(f"[CONSUMER] Flushed batch of {len(batch)} records -> s3://{BRONZE_BUCKET}/{s3_key}")

if __name__ == '__main__':
    print("Starting Kafka S3 Sink Consumer... Press Ctrl+C to stop.")
    buffer = []
    
    for message in consumer:
        buffer.append(message.value)
        print(f"[CONSUMER] Received event {len(buffer)}/{BATCH_SIZE}")
        
        if len(buffer) >= BATCH_SIZE:
            upload_batch_to_s3(buffer)
            buffer = []