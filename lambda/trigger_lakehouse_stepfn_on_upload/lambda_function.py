import json
import boto3
import urllib.parse

sfn = boto3.client("stepfunctions")

STEP_FUNCTION_ARN = "arn:aws:states:eu-west-1:371439860588:stateMachine:EcommerceLakehouseOrchestration"

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        raw_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Only process files in the raw zone with .csv or .xlsx extensions
        if not raw_key.startswith("raw/") or not (raw_key.endswith(".csv") or raw_key.endswith(".xlsx")):
            print(f"Skipping unsupported or irrelevant file: {raw_key}")
            continue

        # Extract dataset name: e.g., raw/products/file.xlsx → products
        try:
            dataset = raw_key.split("/")[1]
        except IndexError:
            print(f"Could not determine dataset from key: {raw_key}")
            continue

        input_payload = {
            "raw_keys": [raw_key],
            "dataset_names": [dataset]
        }

        try:
            response = sfn.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps(input_payload)
            )
            print(f" Step Function started for {raw_key}")
            print(f"Execution ARN: {response['executionArn']}")
        except Exception as e:
            print(f" Failed to start Step Function for {raw_key}: {str(e)}")

    return {
        "statusCode": 200,
        "body": json.dumps("Lambda trigger execution complete.")
    }
