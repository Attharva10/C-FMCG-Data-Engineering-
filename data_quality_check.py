from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FMCG-Data-Quality").getOrCreate()

PATH = "hdfs://namenode:8020/fmcg/processed/sales"

df = spark.read.parquet(PATH)

print("=" * 60)
print("FMCG DATA QUALITY CHECK")
print("=" * 60)

# 1. Total records
print(f"Total records: {df.count():,}")

# 2. Null checks
print("\nNULL VALUE CHECK:")

for column in ["sale_date", "store_id", "sku_id", "units_sold", "net_sales"]:
    count = df.filter(col(column).isNull()).count()
    print(f"{column}: {count}")

# 3. Duplicate check
total_rows = df.count()
distinct_rows = df.dropDuplicates().count()
print(f"\nDuplicate records: {total_rows - distinct_rows:,}")

# 4. Invalid values
negative_sales = df.filter(col("net_sales") < 0).count()
invalid_units = df.filter(col("units_sold") < 0).count()

print(f"Negative sales records: {negative_sales}")
print(f"Negative units records: {invalid_units}")

print("=" * 60)
print("DATA QUALITY CHECK COMPLETE")
print("=" * 60)

spark.stop()
