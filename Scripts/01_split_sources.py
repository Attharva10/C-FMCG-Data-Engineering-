import pandas as pd
from pathlib import Path

# Project folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input file
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "fmcg_sales_3years_1M_rows.csv"

# Output folders
TRANSACTIONS_DIR = PROJECT_ROOT / "data" / "transactions"
PRODUCTS_DIR = PROJECT_ROOT / "data" / "products"
STORES_DIR = PROJECT_ROOT / "data" / "stores"

# Create folders if they don't exist
TRANSACTIONS_DIR.mkdir(parents=True, exist_ok=True)
PRODUCTS_DIR.mkdir(parents=True, exist_ok=True)
STORES_DIR.mkdir(parents=True, exist_ok=True)

print("Loading dataset...")

df = pd.read_csv(INPUT_FILE)

print("Dataset loaded successfully!")
print(f"Rows: {len(df):,}")
print(f"Columns: {len(df.columns)}")


# -----------------------------
# TRANSACTIONS
# -----------------------------

transaction_columns = [
    "date",
    "store_id",
    "sku_id",
    "units_sold",
    "list_price",
    "discount_pct",
    "promo_flag",
    "gross_sales",
    "net_sales",
    "stock_on_hand",
    "stock_out_flag"
]

transactions = df[transaction_columns].copy()

transactions.to_csv(
    TRANSACTIONS_DIR / "transactions_source.csv",
    index=False
)


# -----------------------------
# PRODUCT MASTER
# -----------------------------

product_columns = [
    "sku_id",
    "sku_name",
    "category",
    "subcategory",
    "brand",
    "supplier_id",
    "purchase_cost"
]

products = (
    df[product_columns]
    .drop_duplicates(subset=["sku_id"])
    .copy()
)

products.to_csv(
    PRODUCTS_DIR / "product_master.csv",
    index=False
)


# -----------------------------
# STORE MASTER
# -----------------------------

store_columns = [
    "store_id",
    "country",
    "city",
    "channel",
    "latitude",
    "longitude"
]

stores = (
    df[store_columns]
    .drop_duplicates(subset=["store_id"])
    .copy()
)

stores.to_csv(
    STORES_DIR / "store_master.csv",
    index=False
)


# -----------------------------
# RESULT
# -----------------------------

print()
print("====================================")
print("SOURCE SPLIT COMPLETE")
print("====================================")

print(f"Transactions : {len(transactions):,}")
print(f"Products     : {len(products):,}")
print(f"Stores       : {len(stores):,}")

print()
print("Files created:")
print("data/transactions/transactions_source.csv")
print("data/products/product_master.csv")
print("data/stores/store_master.csv")