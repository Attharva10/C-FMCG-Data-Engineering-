import pandas as pd
from pathlib import Path

# Project root = FMCG-Data-Engineering
project_root = Path(__file__).resolve().parent.parent

# Build the CSV path
file_path = project_root / "data" / "raw" / "fmcg_sales_3years_1M_rows.csv"

print("CSV path:")
print(file_path)

print("\nDoes file exist?")
print(file_path.exists())

# Read CSV
df = pd.read_csv(file_path)

print("\n================================")
print("DATASET SHAPE")
print("================================")
print(df.shape)

print("\n================================")
print("COLUMN NAMES")
print("================================")
print(df.columns.tolist())

print("\n================================")
print("FIRST 5 ROWS")
print("================================")
print(df.head())