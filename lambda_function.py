import json
import os
import boto3

sns_client = boto3.client("sns")
SIZE_THRESHOLD_BYTES = 3000
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def lambda_handler(event, context):
    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        object_size = record["s3"]["object"]["size"]
        print(f"Ingested S3 File: s3://{bucket_name}/{object_key} | Size: {object_size} bytes")
        if object_size > SIZE_THRESHOLD_BYTES:
            message = (
                f"ALERT: High Volume Data Ingestion Detected!\n\n"
                f"S3 Bucket: {bucket_name}\n"
                f"File Key: {object_key}\n"
                f"File Size: {object_size / 1024:.2f} KB\n"
                f"Threshold Limit: {SIZE_THRESHOLD_BYTES / 1024:.2f} KB\n\n"
                f"Ingestion pipeline continues uninterrupted."
            )
            if SNS_TOPIC_ARN:
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject="[AWS Alert] Ingestion Volume Threshold Exceeded",
                    Message=message
                )
                print("SNS Notification sent successfully.")
    return {"statusCode": 200, "body": json.dumps("Threshold monitoring completed.")}
