from google.cloud import storage

client = storage.Client()

bucket = "gs://spark_data1"
file = "gs://spark_data1/amazon_reviews.csv"
bucket = client.get_bucket('spark_data1')
# blob = bucket.get_blob("amazon_reviews.csv")

# file_content = blob.download_as_string().decode('utf-8')

# print(file_content)

from pyspark.sql import Row
# need to import for session creation
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql.functions import col , avg , count, stddev_pop, length, replace, size , split , regexp_replace

spark = SparkSession.builder.getOrCreate()
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\hi\Downloads\vijay-410011-a98c6db25f77.json"
spark = SparkSession.builder \
    .appName('spark-run-with-gcp-bucket') \
    .config("spark.jars", r"C:\Users\hi\Downloads\gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()
    
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
df = spark \
    .read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv("gs://spark_data1/amazon_reviews.csv")

# avg_column.show()
# std_column = df.select(stddev_pop("price"))
# # std_column.show()

# # Frequent_values = df.groupBy("categoryName").count().orderBy("count", ascending = False).limit(5).show()

# # df = df.withColumnRenamed("price", "product_price")

# # df = df.selectExpr("price as productprice")

# # new_df.show()
# # df.show()
# # df.summary("min", "25%", "75%", "mean").show()


# word_count = df.withColumn("word_count",(length(col("categoryName")) - length(regexp_replace(col("categoryName"), " ", ""))+1))

# word_count.select("asin","categoryName", "word_count").show()
