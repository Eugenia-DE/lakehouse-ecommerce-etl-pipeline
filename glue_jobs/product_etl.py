import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_key", "dataset_name"])
RAW_KEY = args["raw_key"]
DATASET = args["dataset_name"]
JOB_NAME = args["JOB_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Paths
bucket = "ecommerce-lakehouse-project"
RAW_PATH = f"s3://{bucket}/{RAW_KEY}"
PROCESSED_PATH = f"s3://{bucket}/processed/{DATASET}/"
REJECTED_PATH = f"{PROCESSED_PATH}/rejected_records/"
ARCHIVE_PATH = f"s3://{bucket}/archived/{RAW_KEY}"
DATABASE_NAME = "ecommerce_lakehouse"
TABLE_NAME = DATASET

# Schema definition (for products)
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Read raw data
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(RAW_PATH)

# Validation
required_fields = ["product_id", "department_id", "department", "product_name"]
df_valid = df_raw.dropna(subset=required_fields)
df_invalid = df_raw.subtract(df_valid)

# Deduplication
df_clean = df_valid.dropDuplicates(["product_id"])

# Add metadata
df_clean = df_clean.withColumn("ingestion_timestamp", current_timestamp())

# Write invalid Rows
if df_invalid.count() > 0:
    df_invalid.withColumn("rejection_reason", lit("Missing required fields")) \
        .write.mode("overwrite").option("header", "true") \
        .csv(REJECTED_PATH)

# Delta Lake Merge (Upsert)
if DeltaTable.isDeltaTable(spark, PROCESSED_PATH):
    delta_table = DeltaTable.forPath(spark, PROCESSED_PATH)

    delta_table.alias("target").merge(
        df_clean.alias("source"),
        "target.product_id = source.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    df_clean.write.format("delta") \
        .partitionBy("department") \
        .mode("overwrite") \
        .save(PROCESSED_PATH)

# Glue catalog registration
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME}
    USING DELTA
    LOCATION '{PROCESSED_PATH}'
""")

# Archive raw file
s3 = boto3.resource("s3")
source_bucket = s3.Bucket(bucket)
copy_source = {"Bucket": bucket, "Key": RAW_KEY}
source_bucket.copy(copy_source, f"archived/{RAW_KEY}")
s3.Object(bucket, RAW_KEY).delete()

job.commit()
