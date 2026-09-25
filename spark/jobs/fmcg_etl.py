from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, when


# =========================================================
# 1. SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("FMCG-Sales-ETL")
    .getOrCreate()
)

print("=" * 60)
print("FMCG PYSPARK ETL STARTED")
print("=" * 60)


# =========================================================
# 2. PATHS
# =========================================================

BASE_PATH = "hdfs://namenode:8020/fmcg"

TRANSACTIONS_PATH = f"{BASE_PATH}/raw/transactions/*.csv"
PRODUCTS_PATH = f"{BASE_PATH}/raw/products/product_master.csv"
STORES_PATH = f"{BASE_PATH}/raw/stores/store_master.csv"

OUTPUT_SALES = f"{BASE_PATH}/processed/sales"
OUTPUT_PRODUCTS = f"{BASE_PATH}/processed/products"
OUTPUT_STORES = f"{BASE_PATH}/processed/stores"


# =========================================================
# 3. READ TRANSACTIONS
# =========================================================

print("\nReading transactions...")

transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(TRANSACTIONS_PATH)
)

print(f"Transaction rows: {transactions.count():,}")


# =========================================================
# 4. READ PRODUCTS
# =========================================================

print("\nReading products...")

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTS_PATH)
)

print(f"Product rows: {products.count():,}")


# =========================================================
# 5. READ STORES
# =========================================================

print("\nReading stores...")

stores = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(STORES_PATH)
)

print(f"Store rows: {stores.count():,}")


# =========================================================
# 6. CLEAN TRANSACTIONS
# =========================================================

print("\nCleaning transactions...")

transactions_clean = (
    transactions
    .withColumn("date", to_date(col("date")))

    .filter(col("date").isNotNull())
    .filter(col("store_id").isNotNull())
    .filter(col("sku_id").isNotNull())

    .withColumn("units_sold", col("units_sold").cast("integer"))
    .withColumn("list_price", col("list_price").cast("double"))
    .withColumn("discount_pct", col("discount_pct").cast("double"))
    .withColumn("gross_sales", col("gross_sales").cast("double"))
    .withColumn("net_sales", col("net_sales").cast("double"))

    .withColumn(
        "discount_amount",
        round(col("gross_sales") - col("net_sales"), 2)
    )

    .withColumn(
        "stock_status",
        when(col("stock_out_flag") == 1, "OUT_OF_STOCK")
        .otherwise("IN_STOCK")
    )

    .dropDuplicates()
)

print(
    f"Clean transaction rows: "
    f"{transactions_clean.count():,}"
)


# =========================================================
# 7. PREPARE PRODUCT DIMENSION
# =========================================================

products_dim = products.select(
    "sku_id",
    "sku_name",
    "category",
    "subcategory",
    "brand",
    "supplier_id",
    "purchase_cost"
)

# Remove duplicate SKU records
products_dim = products_dim.dropDuplicates(["sku_id"])


# =========================================================
# 8. PREPARE STORE DIMENSION
# =========================================================

stores_dim = stores.select(
    "store_id",
    "country",
    "city",
    "channel",
    "latitude",
    "longitude"
)

# Remove duplicate store records
stores_dim = stores_dim.dropDuplicates(["store_id"])


# =========================================================
# 9. JOIN PRODUCT MASTER
# =========================================================

print("\nJoining products...")

sales_enriched = transactions_clean.join(
    products_dim,
    on="sku_id",
    how="left"
)


# =========================================================
# 10. JOIN STORE MASTER
# =========================================================

print("Joining stores...")

sales_enriched = sales_enriched.join(
    stores_dim,
    on="store_id",
    how="left"
)


# =========================================================
# 11. FINAL COLUMNS
# =========================================================

sales_final = sales_enriched.select(
    col("date").alias("sale_date"),
    "store_id",
    "country",
    "city",
    "channel",

    "sku_id",
    "sku_name",
    "category",
    "subcategory",
    "brand",

    "supplier_id",

    "units_sold",
    "list_price",
    "discount_pct",
    "discount_amount",
    "promo_flag",

    "gross_sales",
    "net_sales",

    "purchase_cost",

    "stock_on_hand",
    "stock_out_flag",
    "stock_status"
)


# =========================================================
# 12. VALIDATE BEFORE WRITING
# =========================================================

print("\nFinal schema:")
sales_final.printSchema()

print("\nSample records:")
sales_final.show(5, truncate=False)

print("\nFinal row count:")
final_count = sales_final.count()
print(f"{final_count:,}")


# =========================================================
# 13. WRITE SALES AS PARQUET
# =========================================================

print("\nWriting sales Parquet...")

(
    sales_final
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet(OUTPUT_SALES)
)


# =========================================================
# 14. WRITE PRODUCT PARQUET
# =========================================================

print("Writing product Parquet...")

(
    products_dim
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PRODUCTS)
)


# =========================================================
# 15. WRITE STORE PARQUET
# =========================================================

print("Writing store Parquet...")

(
    stores_dim
    .write
    .mode("overwrite")
    .parquet(OUTPUT_STORES)
)


# =========================================================
# 16. COMPLETE
# =========================================================

print("\n" + "=" * 60)
print("FMCG PYSPARK ETL COMPLETE")
print("=" * 60)

print(f"Final sales rows: {final_count:,}")
print(f"Sales output: {OUTPUT_SALES}")
print(f"Products output: {OUTPUT_PRODUCTS}")
print(f"Stores output: {OUTPUT_STORES}")

spark.stop()