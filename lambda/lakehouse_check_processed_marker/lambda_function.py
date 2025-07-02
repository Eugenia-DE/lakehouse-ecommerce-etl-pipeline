import json
import boto3

s3 = boto3.client("s3")
BUCKET = "ecommerce-lakehouse-project"

def lambda_handler(event, context):
    dataset = event.get("dataset")
    filename = event.get("filename")

    if not dataset or not filename:
        return {
            "statusCode": 400,
            "body": json.dumps("Missing dataset or filename")
        }

    marker_key = f"processed/processed_log/{dataset}/{filename}.txt"

    try:
        s3.head_object(Bucket=BUCKET, Key=marker_key)
        # If object exists, return True
        return { "already_processed": True }
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # Marker doesn't exist
            return { "already_processed": False }
        else:
            raise
