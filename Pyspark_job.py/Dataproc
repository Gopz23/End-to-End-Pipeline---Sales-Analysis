from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("SalesPipeline").getOrCreate()

df = spark.read.csv("gs://your-bucket-name/sales_data.csv", header=True, inferSchema=True)

df_transformed = df.groupBy("customer_id").agg(
    sum("amount").alias("total_spent")
)

df_transformed.write.mode("overwrite").csv("gs://your-bucket-name/output/")

spark.stop()
