from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import logging

# Initialize Spark
spark = SparkSession.builder.appName("GopzSalesPipeline").getOrCreate()

# Logging
logging.basicConfig(level=logging.INFO)
logging.info("Starting PySpark Job...")

# GCS Paths (REAL)
INPUT_PATH = "gs://gopz-sales-data-bucket-2026/data/sales_data.csv"
OUTPUT_PATH = "gs://gopz-sales-data-bucket-2026/output/sales_summary"

# Read data from GCS
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

logging.info("Data Read Successfully")

# Show schema (good for debugging)
df.printSchema()

# Transformation: total spending per customer
df_transformed = df.groupBy("customer_id").agg(
    sum("amount").alias("total_spent")
)

logging.info("Transformation Completed")

# Write output back to GCS
df_transformed.write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)

logging.info("Data Written to GCS Successfully")

# Stop Spark session
spark.stop()

logging.info("PySpark Job Completed")
