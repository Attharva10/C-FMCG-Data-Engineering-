from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, count

spark = SparkSession.builder.appName("diagnose_raw_files").getOrCreate()

path = "hdfs://namenode:8020/fmcg/raw/transactions"

df = (
    spark.read
    .option("header", True)
    .csv(path)
    .withColumn("_file", input_file_name())
)

result = (
    df.groupBy("_file")
      .count()
      .orderBy("_file")
)

result.show(120, truncate=False)

print(f"TOTAL ROWS: {df.count():,}")

spark.stop()
