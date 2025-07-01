import sys
import pandas as pd
import boto3
import io
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_date
from delta import DeltaTable


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "raw_key", "dataset_name"]
    )
    RAW_KEY = args["raw_key"]
    JOB_NAME = args["JOB_NAME"]
    DATASET = args["dataset_name"]

    BUCKET = "ecommerce-lakehouse-project"
    PROCESSED_PATH = f"s3://{BUCKET}/processed/{DATASET}/"
    DATABASE_NAME = "ecommerce_lakehouse"
    TABLE_NAME = DATASET

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session.builder \
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ) \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        .getOrCreate()
    job = Job(glue_context)
    job.init(JOB_NAME, args)

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
    excel_bytes = response["Body"].read()
    excel_file = pd.ExcelFile(io.BytesIO(excel_bytes))

    required_columns = [
        "order_num", "order_id", "user_id",
        "order_timestamp", "total_amount"
    ]
    valid_rows, invalid_rows = [], []

    for sheet in excel_file.sheet_names:
        try:
            df = excel_file.parse(sheet)
            df["date"] = pd.to_datetime(df["order_timestamp"]).dt.date
            df = df[required_columns + ["date"]]
            valid = df.dropna(
                subset=["order_id", "user_id", "order_timestamp"]
            )
            invalid = df[~df.index.isin(valid.index)]
            valid_rows.append(valid)
            invalid_rows.append(invalid)
        except Exception as e:
            print(f"Skipping sheet {sheet} due to: {e}")

    if not valid_rows:
        print("No valid data to process.")
        job.commit()
        return

    df_valid = pd.concat(valid_rows, ignore_index=True)
    spark_df = spark.createDataFrame(df_valid)

    spark_df = spark_df.dropDuplicates(["order_id"]) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn(
            "order_timestamp",
            col("order_timestamp").cast("timestamp")
        ) \
        .withColumn("date", to_date(col("order_timestamp")))

    if DeltaTable.isDeltaTable(spark, PROCESSED_PATH):
        DeltaTable.forPath(spark, PROCESSED_PATH) \
            .alias("target") \
            .merge(
                spark_df.alias("source"),
                "target.order_id = source.order_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        spark_df.write.format("delta") \
            .partitionBy("date") \
            .mode("overwrite") \
            .save(PROCESSED_PATH)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
        f"{DATABASE_NAME}.{TABLE_NAME} USING DELTA LOCATION "
        f"'{PROCESSED_PATH}'"
    )

    print(f"Finished processing {RAW_KEY}")
    job.commit()


main()
