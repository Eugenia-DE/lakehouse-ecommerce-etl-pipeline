import sys
import boto3
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "raw_keys", "dataset_names"]
    )
    JOB_NAME = args["JOB_NAME"]
    RAW_KEYS = args["raw_keys"].split(",")
    DATASET_NAMES = args["dataset_names"].split(",")

    BUCKET = "ecommerce-lakehouse-project"
    glueContext = GlueContext(SparkContext())
    job = Job(glueContext)
    job.init(JOB_NAME, args)

    s3 = boto3.resource("s3")
    s3_client = boto3.client("s3")
    bucket = s3.Bucket(BUCKET)

    for raw_key, dataset in zip(RAW_KEYS, DATASET_NAMES):
        print(f"Archiving and marking: {raw_key} | dataset: {dataset}")
        copy_source = {"Bucket": BUCKET, "Key": raw_key}
        archive_key = f"archived/{raw_key}"
        log_key = (
            f"processed/_processed_log/{dataset}/"
            f"{os.path.basename(raw_key)}.txt"
        )

        try:
            bucket.copy(copy_source, archive_key)
            s3.Object(BUCKET, raw_key).delete()
            s3_client.put_object(
                Bucket=BUCKET,
                Key=log_key,
                Body="processed"
            )
            print(f"Archived {raw_key} and created marker {log_key}")
        except Exception as e:
            print(f"Failed to archive/mark {raw_key}:", str(e))

    job.commit()


main()
