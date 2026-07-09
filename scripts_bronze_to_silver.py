from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()

# Read Bronze table from BigQuery
bronze_df = (
    spark.read
    .format("bigquery")
    .load("carrier-poc-497515.poc_lakehouse.bronze_customer")
)

# Apply Silver layer rules:
# 1. Remove duplicates by customer_id
# 2. Remove records where customer_name is null
silver_df = (
    bronze_df
    .dropDuplicates(["customer_id"])
    .filter(col("customer_name").isNotNull())
)

# Write to Silver Iceberg table
# IMPORTANT: Use append, not overwrite
silver_df.write \
    .format("bigquery") \
    .option("table", "carrier-poc-497515.poc_lakehouse.silver_customer") \
    .option("temporaryGcsBucket", "carrier-lakehouse-demo") \
    .option("parentProject", "carrier-poc-497515") \
    .mode("append") \
    .save()

print("Bronze to Silver completed successfully")
