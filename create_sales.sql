CREATE TABLE IF NOT EXISTS sales (
    sale_date DATE,
    store_id STRING,
    city STRING,
    channel STRING,
    sku_id STRING,
    sku_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    supplier_id STRING,
    units_sold INT,
    list_price DOUBLE,
    discount_pct DOUBLE,
    discount_amount DOUBLE,
    promo_flag INT,
    gross_sales DOUBLE,
    net_sales DOUBLE,
    purchase_cost DOUBLE,
    stock_on_hand INT,
    stock_out_flag INT,
    stock_status STRING
)
PARTITIONED BY (country STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/fmcg/processed/sales';

MSCK REPAIR TABLE sales;

