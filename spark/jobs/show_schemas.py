from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShowSchemas") \
    .getOrCreate()

paths = {
    "Sales": "hdfs://namenode:8020/fmcg/processed/sales",
    "Products": "hdfs://namenode:8020/fmcg/processed/products",
    "Stores": "hdfs://namenode:8020/fmcg/processed/stores"
}

for name, path in paths.items():
    print(f"\n===== {name} Schema =====")
    df = spark.read.parquet(path)
    df.printSchema()
    print(f"Records: {df.count()}")

spark.stop()