import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType
from delta import DeltaTable


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "raw_key", "dataset_name"]
    )
    RAW_KEY = args["raw_key"]
    DATASET = args["dataset_name"]
    JOB_NAME = args["JOB_NAME"]

    sc = SparkContext()
    spark = SparkSession.builder \
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ) \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        .getOrCreate()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(JOB_NAME, args)

    bucket = "ecommerce-lakehouse-project"
    raw_path = f"s3://{bucket}/{RAW_KEY}"
    processed_path = f"s3://{bucket}/processed/{DATASET}/"
    rejected_path = f"{processed_path}/rejected_records/"
    database_name = "ecommerce_lakehouse"
    table_name = DATASET

    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("department_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True)
    ])

    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(raw_path)

    required_fields = [
        "product_id", "department_id", "department", "product_name"
    ]
    df_valid = df_raw.dropna(subset=required_fields)
    df_invalid = df_raw.subtract(df_valid)

    df_clean = df_valid.dropDuplicates(["product_id"]).withColumn(
        "ingestion_timestamp", current_timestamp()
    )

    if df_invalid.count() > 0:
        df_invalid.withColumn(
            "rejection_reason",
            lit("Missing required fields")
        ).write.mode("overwrite") \
         .option("header", "true") \
         .csv(rejected_path)

    if DeltaTable.isDeltaTable(spark, processed_path):
        DeltaTable.forPath(spark, processed_path) \
            .alias("target") \
            .merge(
                df_clean.alias("source"),
                "target.product_id = source.product_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df_clean.write.format("delta") \
            .partitionBy("department") \
            .mode("overwrite") \
            .save(processed_path)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING DELTA
        LOCATION '{processed_path}'
    """)

    print(f"Finished processing {RAW_KEY}")
    job.commit()


main()
