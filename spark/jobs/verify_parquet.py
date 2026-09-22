from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Verify-Parquet")
    .getOrCreate()
)


path = "hdfs://namenode:8020/fmcg/processed/sales"

print("=" * 60)
print("PROCESSED PARQUET VERIFICATION")
print("=" * 60)

print("\nReading:")
print(path)

df = spark.read.parquet(path)

print("\nNumber of rows:")
print(df.count())

print("\nSchema:")
df.printSchema()

print("\nFirst 5 records:")
df.show(5, truncate=False)

print("\n" + "=" * 60)
print("PARQUET VERIFICATION COMPLETE")
print("=" * 60)

spark.stop()