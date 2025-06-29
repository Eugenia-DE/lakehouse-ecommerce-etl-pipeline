import sys
import pandas as pd
import boto3
import io
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_date
from delta import DeltaTable

# Glue Initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_key"])
RAW_KEY = args["raw_key"]
JOB_NAME = args["JOB_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Configurations
BUCKET = "ecommerce-lakehouse-project"
RAW_PATH = f"s3://{BUCKET}/{RAW_KEY}"
PROCESSED_PATH = f"s3://{BUCKET}/processed/order_items/"
REJECTED_PATH = f"s3://{BUCKET}/processed/order_items/rejected_records/"
MARKER_LOG_PATH = f"processed/_processed_log/order_items/{os.path.basename(RAW_KEY)}.txt"
ARCHIVE_PATH = f"s3://{BUCKET}/archived/{RAW_KEY}"
DATABASE_NAME = "ecommerce_lakehouse"
TABLE_NAME = "order_items"

ORDERS_PATH = f"s3://{BUCKET}/processed/orders/"  # FK source

s3 = boto3.client("s3")

# Idempotency Check
try:
    s3.head_object(Bucket=BUCKET, Key=MARKER_LOG_PATH)
    print(f"Skipping job. File already processed: {RAW_KEY}")
    job.commit()
    sys.exit(0)
except s3.exceptions.ClientError:
    print("No marker found. Processing...")

# Load Reference FK Data (orders)
orders_df = spark.read.format("delta").load(ORDERS_PATH)
valid_order_ids = orders_df.select("order_id").distinct()

# Read Excel from S3
response = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
excel_bytes = response["Body"].read()
excel_file = pd.ExcelFile(io.BytesIO(excel_bytes))

required_columns = [
    "id", "order_id", "user_id", "days_since_prior_order",
    "product_id", "add_to_cart_order", "reordered", "order_timestamp"
]

valid_rows = []
invalid_rows = []

for sheet_name in excel_file.sheet_names:
    try:
        df_sheet = excel_file.parse(sheet_name)
        df_sheet["date"] = pd.to_datetime(df_sheet["order_timestamp"]).dt.date
        df_sheet = df_sheet[required_columns + ["date"]]

        # Basic Validation
        valid = df_sheet.dropna(subset=["id", "order_id", "product_id", "user_id", "order_timestamp"])
        invalid = df_sheet[~df_sheet.index.isin(valid.index)]
        valid_rows.append(valid)
        invalid_rows.append(invalid)
    except Exception as e:
        print(f"Skipping sheet {sheet_name} due to error: {e}")

# Convert to Spark DataFrame
df_valid = pd.concat(valid_rows, ignore_index=True)
df_invalid = pd.concat(invalid_rows, ignore_index=True)

if df_valid.empty:
    print("No valid rows to process.")
    job.commit()
    sys.exit(0)

spark_df = spark.createDataFrame(df_valid)

# Referential Integrity Check (order_id exists in orders)
spark_df = spark_df.join(valid_order_ids, on="order_id", how="leftsemi")

# Clean and enrich
spark_df = spark_df.dropDuplicates(["id"])
spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp())
spark_df = spark_df.withColumn("order_timestamp", col("order_timestamp").cast("timestamp"))
spark_df = spark_df.withColumn("date", to_date(col("order_timestamp")))

# Delta Merge/Upsert
if DeltaTable.isDeltaTable(spark, PROCESSED_PATH):
    delta_table = DeltaTable.forPath(spark, PROCESSED_PATH)
    delta_table.alias("target").merge(
        spark_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    spark_df.write.format("delta") \
        .partitionBy("date") \
        .mode("overwrite") \
        .save(PROCESSED_PATH)
    
# Glue Catalog Registration
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME}
    USING DELTA
    LOCATION '{PROCESSED_PATH}'
""")

# Write Rejected Rows (Optional)
if not df_invalid.empty:
    rejected_spark_df = spark.createDataFrame(df_invalid)
    rejected_spark_df.write.mode("overwrite").option("header", "true").csv(REJECTED_PATH)

# Archive File & Write Marker
s3_resource = boto3.resource("s3")
bucket_resource = s3_resource.Bucket(BUCKET)
copy_source = {"Bucket": BUCKET, "Key": RAW_KEY}
bucket_resource.copy(copy_source, f"archived/{RAW_KEY}")
s3_resource.Object(BUCKET, RAW_KEY).delete()

s3.put_object(Bucket=BUCKET, Key=MARKER_LOG_PATH, Body="processed")

print(f"Successfully processed: {RAW_KEY}")
job.commit()
