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
    ORDERS_PATH = f"s3://{BUCKET}/processed/orders/"

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

    try:
        orders_df = spark.read.format("delta").load(ORDERS_PATH)
        valid_order_ids = orders_df.select("order_id").distinct()
    except Exception as e:
        print("Failed to load FK orders table:", str(e))
        job.commit()
        return

    try:
        products_df = spark.read.format("delta").load(
            f"s3://{BUCKET}/processed/products/"
        )
        valid_product_ids = products_df.select("product_id").distinct()
    except Exception as e:
        print("Failed to load products Delta table:", str(e))
        job.commit()
        return

    response = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
    excel_bytes = response["Body"].read()
    excel_file = pd.ExcelFile(io.BytesIO(excel_bytes))

    required_columns = [
        "id", "order_id", "user_id", "days_since_prior_order",
        "product_id", "add_to_cart_order", "reordered",
        "order_timestamp"
    ]
    valid_rows, invalid_rows = [], []

    for sheet in excel_file.sheet_names:
        try:
            df = excel_file.parse(sheet)
            df["date"] = pd.to_datetime(
                df["order_timestamp"]
            ).dt.date
            df = df[required_columns + ["date"]]
            valid = df.dropna(
                subset=[
                    "id", "order_id", "product_id",
                    "user_id", "order_timestamp"
                ]
            )
            invalid = df[~df.index.isin(valid.index)]
            valid_rows.append(valid)
            invalid_rows.append(invalid)
        except Exception as e:
            print(f"Skipping sheet {sheet} due to error: {e}")

    if not valid_rows:
        print("No valid data found.")
        job.commit()
        return

    df_valid = pd.concat(valid_rows, ignore_index=True)
    spark_df = spark.createDataFrame(df_valid)

    spark_df = spark_df.join(
        valid_order_ids, on="order_id", how="leftsemi"
    ).join(
        valid_product_ids, on="product_id", how="leftsemi"
    ).dropDuplicates(["id"]) \
     .withColumn("ingestion_timestamp", current_timestamp()) \
     .withColumn("order_timestamp", col("order_timestamp").cast("timestamp")) \
     .withColumn("date", to_date(col("order_timestamp")))

    if DeltaTable.isDeltaTable(spark, PROCESSED_PATH):
        DeltaTable.forPath(spark, PROCESSED_PATH) \
            .alias("target") \
            .merge(
                spark_df.alias("source"),
                "target.id = source.id"
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
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME}
        USING DELTA
        LOCATION '{PROCESSED_PATH}'
    """)

    print(f"Finished processing {RAW_KEY}")
    job.commit()


main()
