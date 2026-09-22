from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FMCG-HDFS-Test")
    .getOrCreate()
)

print("========================================")
print("SPARK → HDFS TEST")
print("========================================")

hdfs_file = (
    "hdfs://namenode:8020"
    "/fmcg/raw/products/product_master.csv"
)

print("Reading:")
print(hdfs_file)

df = spark.read.csv(
    hdfs_file,
    header=True,
    inferSchema=True
)

print("\nSchema:")
df.printSchema()

print("\nFirst 5 rows:")
df.show(5, truncate=False)

print("\nRow count:")
print(df.count())

print("\n========================================")
print("SPARK → HDFS TEST SUCCESS")
print("========================================")

spark.stop()