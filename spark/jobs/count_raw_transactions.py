from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("count_raw_transactions").getOrCreate()

path = "hdfs://namenode:8020/fmcg/raw/transactions"

df = spark.read.option("header", True).csv(path)

print(f"RAW HDFS ROWS: {df.count():,}")

spark.stop()