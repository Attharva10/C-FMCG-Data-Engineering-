# FMCG Global Sales Data Engineering Pipeline

> End-to-end Big Data engineering project: **Kafka → HDFS → PySpark →
> Parquet → Hive → Airflow → Streamlit**

![Python](https://img.shields.io/badge/Python-3.x-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-black)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange)
![HDFS](https://img.shields.io/badge/Hadoop-HDFS-yellow)
![Hive](https://img.shields.io/badge/Apache%20Hive-Analytics-red)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-blue)
![Docker](https://img.shields.io/badge/Docker-Containers-2496ED)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B)

## 📌 Overview

The **FMCG Global Sales Data Engineering Pipeline** is an end-to-end
data engineering project that processes large-scale FMCG sales data from
raw CSV files into analytics-ready datasets and an interactive
dashboard.

The pipeline demonstrates:

-   Streaming ingestion with Apache Kafka
-   Distributed raw storage with Hadoop HDFS
-   Large-scale ETL with Apache Spark / PySpark
-   Columnar storage using Parquet
-   Automated data-quality validation
-   SQL analytics with Apache Hive
-   Workflow orchestration with Apache Airflow
-   Containerized infrastructure using Docker
-   Business analytics through Streamlit and Plotly

The final tested pipeline processed approximately **1.1 million sales
records**, **102 products**, and **13 stores** across **7 countries**
for the **2021--2023** period.

------------------------------------------------------------------------

## 🏗️ Architecture

``` text
                    FMCG Source Data
                           │
                           ▼
                  ┌─────────────────┐
                  │  CSV / Sources  │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ Apache Kafka    │
                  │ Ingestion       │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ Hadoop HDFS     │
                  │ Raw Data Layer  │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ Apache Spark    │
                  │ PySpark ETL     │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ Parquet         │
                  │ Processed Data  │
                  └───────┬─┬───────┘
                          │ │
              ┌───────────┘ └────────────┐
              ▼                          ▼
      ┌────────────────┐        ┌────────────────┐
      │ Data Quality   │        │ Apache Hive    │
      │ Validation     │        │ SQL Analytics  │
      └────────┬───────┘        └───────┬────────┘
               │                        │
               └──────────┬─────────────┘
                          ▼
                ┌────────────────────┐
                │ Streamlit Dashboard│
                └────────────────────┘

          Apache Airflow orchestrates the workflow
```

------------------------------------------------------------------------

## 🎯 Business Problem

FMCG companies generate large volumes of transactions across stores,
products, locations, categories, and sales channels. Raw CSV data is
difficult to manage directly for scalable analytics.

This project builds a complete data pipeline that:

1.  Ingests transaction data.
2.  Stores raw data in a distributed storage layer.
3.  Cleans and enriches the data.
4.  Validates data quality.
5.  Creates an analytical SQL layer.
6.  Automates the workflow.
7.  Presents business insights in a dashboard.

------------------------------------------------------------------------

## 🎯 Objectives

-   Build an end-to-end Data Engineering pipeline.
-   Demonstrate Kafka-based ingestion.
-   Use HDFS as a raw data lake.
-   Process more than one million records with PySpark.
-   Join transactions with product and store master data.
-   Store processed data as partitioned Parquet.
-   Validate missing values, duplicates, and invalid measures.
-   Build Hive analytical tables.
-   Orchestrate the pipeline with Airflow.
-   Build an interactive sales dashboard.
-   Make the project reproducible through Docker and GitHub.

------------------------------------------------------------------------

# 📊 Dataset

The local dataset contains:

  ------------------------------------------------------------------------------------
  Dataset                                       Approximate size Purpose
  --------------------------------- ---------------------------- ---------------------
  `fmcg_sales_3years_1M_rows.csv`                       \~212 MB Main raw FMCG sales
                                                                 dataset

  `transactions_source.csv`                              \~70 MB Transaction source

  `product_master.csv`                                    \~6 KB Product master

  `store_master.csv`                                      \~1 KB Store master
  ------------------------------------------------------------------------------------

Processed data contains approximately:

-   **1,100,000 sales records**
-   **102 products**
-   **13 stores**
-   **7 countries**
-   **2021--2023** sales period

Countries include Austria, France, Germany, Italy, Netherlands, Poland,
and Spain.

### Local data structure

``` text
Data/
├── raw/
│   └── fmcg_sales_3years_1M_rows.csv
├── transactions/
│   └── transactions_source.csv
├── products/
│   └── product_master.csv
└── stores/
    └── store_master.csv
```

> **Important:** `Data/` is intentionally excluded from GitHub because
> the raw datasets are large. Users must obtain the required data
> separately and place it in the paths above.

------------------------------------------------------------------------

# 🛠️ Technology Stack

  Technology                Purpose
  ------------------------- -------------------------------
  Python                    Scripts and application logic
  Apache Kafka              Streaming/message ingestion
  ZooKeeper                 Kafka coordination
  Hadoop HDFS               Raw distributed storage
  Apache Spark / PySpark    ETL and transformation
  Parquet                   Processed columnar storage
  Apache Hive               SQL analytics
  Apache Airflow            Workflow orchestration
  Docker / Docker Compose   Infrastructure
  Streamlit                 Dashboard
  Plotly                    Charts
  Git / GitHub              Version control

------------------------------------------------------------------------

# 🔄 End-to-End Data Flow

## 1. Source

Transaction, product, and store data start as CSV files.

## 2. Kafka

The Kafka producer publishes transaction records to a Kafka topic.

``` text
CSV → Kafka Producer → Kafka Topic
```

Consumers read messages and write the raw transaction data to HDFS.

Kafka files:

``` text
kafka/
├── Dockerfile
├── producer.py
├── consumer.py
├── consumer_hdfs.py
└── consumer_recovery.py
```

> Do not repeatedly run the producer against the same source data unless
> a new ingestion is intended, because it can create duplicate messages.

## 3. HDFS

Raw transaction data is stored under:

``` text
/fmcg/raw/transactions
```

Processed outputs are stored under:

``` text
/fmcg/processed/sales
/fmcg/processed/products
/fmcg/processed/stores
```

## 4. Spark ETL

`spark/jobs/fmcg_etl.py` performs the main processing:

1.  Reads raw HDFS data.
2.  Reads product master data.
3.  Reads store master data.
4.  Cleans and casts fields.
5.  Converts the transaction date to `sale_date`.
6.  Enriches transactions with product information.
7.  Enriches transactions with store information.
8.  Produces analytics-ready sales records.
9.  Writes Parquet output.

Sales are partitioned by country:

``` text
processed/sales/
├── country=Austria/
├── country=France/
├── country=Germany/
├── country=Italy/
├── country=Netherlands/
├── country=Poland/
└── country=Spain/
```

## 5. Parquet

Parquet is used for processed data because it provides columnar storage,
compression, schema preservation, and efficient analytical access.

## 6. Data Quality

The pipeline validates:

-   Required fields
-   Missing values
-   Duplicate records
-   Negative sales
-   Negative units sold

Final validation:

  Check                        Result
  ------------------- ---------------
  Total records         **1,100,000**
  Missing values                **0**
  Duplicate records             **0**
  Negative sales                **0**
  Negative units                **0**

Scripts:

``` text
data_quality_check.py
spark/jobs/data_quality_check.py
```

## 7. Hive

Hive provides the SQL analytics layer over processed Parquet data.

Database:

``` text
fmcg
```

The project uses sales analytics and a `sales_summary` aggregation for
dashboard queries.

Analytics include:

-   Total sales
-   Total units
-   Sales by country
-   Sales by store
-   Monthly sales
-   Product performance
-   Country-level details

## 8. Airflow

DAG:

``` text
fmcg_sales_pipeline
```

Workflow:

``` text
start
  ↓
kafka_ingestion
  ↓
spark_etl
  ↓
data_quality_check
  ↓
hive_analytics
  ↓
end
```

All stages were successfully executed in the final end-to-end test.

## 9. Streamlit

Dashboard:

``` text
dashboard/app.py
```

Start it with:

``` powershell
python -m streamlit run .\dashboardpp.py
```

Open:

``` text
http://localhost:8501
```

Dashboard filters:

-   Year
-   Country
-   Store

Dashboard KPIs:

-   Total Sales
-   Total Units Sold
-   Countries
-   Average Sale / Unit
-   Products

Charts/tables:

-   Sales by Country
-   Country Sales Details
-   Units Sold by Country
-   Sales by Store
-   Monthly Sales Trend
-   Top 10 Products by Sales
-   Top Product Details
-   Country-Level Sales Data
-   Pipeline/Data Quality Health

------------------------------------------------------------------------

# 🐳 Docker Architecture

Docker runs the main infrastructure as containers:

``` text
ZooKeeper
Kafka
HDFS NameNode
HDFS DataNode
Spark
Hive
Airflow
Airflow Scheduler
```

Main configuration:

``` text
docker/
├── docker-compose.yml
└── hadoop-conf/
    ├── core-site.xml
    ├── hdfs-site.xml
    └── hive-site.xml
```

------------------------------------------------------------------------

# 📁 Repository Structure

``` text
FMCG-Data-Engineering/
│
├── airflow/
│   ├── dags/
│   │   └── fmcg_pipeline.py
│   └── webserver_config.py
│
├── dashboard/
│   └── app.py
│
├── docker/
│   ├── docker-compose.yml
│   └── hadoop-conf/
│       ├── core-site.xml
│       ├── hdfs-site.xml
│       └── hive-site.xml
│
├── kafka/
│   ├── Dockerfile
│   ├── producer.py
│   ├── consumer.py
│   ├── consumer_hdfs.py
│   └── consumer_recovery.py
│
├── spark/
│   └── jobs/
│       ├── count_raw_transactions.py
│       ├── data_quality_check.py
│       ├── diagnose_raw_files.py
│       ├── fmcg_etl.py
│       ├── show_schemas.py
│       ├── test_hdfs.py
│       └── verify_parquet.py
│
├── Scripts/
│   ├── 00_inspect_data.py
│   └── 01_split_sources.py
│
├── create_sales.sql
├── data_quality_check.py
├── docker-compose.yml
├── .gitignore
└── README.md
```

Generated runtime files and large data are intentionally excluded.

------------------------------------------------------------------------

# 💻 Installation & Setup

## Prerequisites

Install:

-   Git
-   Docker Desktop
-   Python 3.x
-   PowerShell

Recommended:

-   8 GB+ RAM
-   4+ CPU cores
-   Sufficient free disk space for Docker, HDFS, Spark, and the dataset

## 1. Clone

``` powershell
git clone https://github.com/Attharva10/C-FMCG-Data-Engineering-.git
cd C-FMCG-Data-Engineering-
```

## 2. Prepare data

Create:

``` powershell
mkdir Data
mkdir Dataaw
mkdir Data  ransactions
mkdir Data\products
mkdir Data\stores
```

Place:

``` text
Data/raw/fmcg_sales_3years_1M_rows.csv
Data/transactions/transactions_source.csv
Data/products/product_master.csv
Data/stores/store_master.csv
```

## 3. Start Docker

``` powershell
docker compose -f .\docker\docker-compose.yml up -d
```

Verify:

``` powershell
docker ps
```

## 4. Inspect data

``` powershell
python .\Scripts _inspect_data.py
```

If source preparation is required:

``` powershell
python .\Scripts_split_sources.py
```

## 5. Run the pipeline

The recommended production-style execution is through Airflow.

Open:

``` text
http://localhost:8080
```

Open:

``` text
fmcg_sales_pipeline
```

Trigger the DAG.

Expected sequence:

``` text
start
→ kafka_ingestion
→ spark_etl
→ data_quality_check
→ hive_analytics
→ end
```

## 6. Start dashboard

Install dashboard packages:

``` powershell
python -m pip install streamlit pandas plotly
```

Run:

``` powershell
python -m streamlit run .\dashboardpp.py
```

Open:

``` text
http://localhost:8501
```

------------------------------------------------------------------------

# 🔍 Verification

The completed environment was verified with:

``` text
Processed sales records : 1,100,000
Products                : 102
Stores                  : 13
Countries               : 7
Sales period            : 2021–2023
```

Quality:

``` text
Missing values          : 0
Duplicate records       : 0
Negative sales          : 0
Negative units          : 0
```

Final Airflow run:

``` text
start                SUCCESS
kafka_ingestion      SUCCESS
spark_etl            SUCCESS
data_quality_check   SUCCESS
hive_analytics       SUCCESS
end                  SUCCESS
```

------------------------------------------------------------------------

# 🧪 Useful Scripts

  Script                                   Purpose
  ---------------------------------------- -------------------------
  `Scripts/00_inspect_data.py`             Inspect source data
  `Scripts/01_split_sources.py`            Prepare source data
  `spark/jobs/test_hdfs.py`                Test HDFS
  `spark/jobs/count_raw_transactions.py`   Count raw HDFS records
  `spark/jobs/diagnose_raw_files.py`       Diagnose raw files
  `spark/jobs/show_schemas.py`             Inspect schemas
  `spark/jobs/verify_parquet.py`           Verify Parquet
  `spark/jobs/data_quality_check.py`       Validate processed data

------------------------------------------------------------------------

# 🧩 Challenges & Solutions

### Large dataset

The raw CSV files are hundreds of MB, so they are excluded from GitHub.

**Solution:** keep large data local and version-control the pipeline
code/configuration.

### Streaming ingestion

The source begins as CSV while the architecture demonstrates Kafka
ingestion.

**Solution:** Kafka producer publishes transaction records and consumers
persist them into HDFS.

### Large-scale ETL

More than one million records need repeatable processing.

**Solution:** PySpark handles cleaning, joins, enrichment, and Parquet
generation.

### Schema mismatch during development

The processed field was changed from `date` to `sale_date`, while the
quality check still referenced `date`.

**Solution:** update the quality check to use the final processed
schema. The validation then completed successfully.

### Hive partition/path issue

Hive vectorized execution produced invalid input path errors against the
partitioned Parquet data.

**Solution:** disable Hive vectorized execution for the affected
analytics queries:

``` sql
SET hive.vectorized.execution.enabled=false;
```

### Dashboard performance

Querying all transaction-level data repeatedly is unnecessary for
dashboard charts.

**Solution:** create and query a `sales_summary` aggregation for
dashboard analytics.

------------------------------------------------------------------------

# ❓ Why These Technologies?

### Kafka

Provides a streaming/message ingestion layer and decouples producers
from consumers.

### HDFS

Provides distributed storage for the raw data layer.

### Spark

Provides scalable transformation, joins, cleaning, and enrichment.

### Parquet

Provides efficient columnar storage for analytics.

### Hive

Provides SQL-based analytical access to processed data.

### Airflow

Provides scheduling, dependency management, monitoring, and
orchestration.

### Docker

Provides isolated and repeatable infrastructure services.

### Streamlit

Provides an interactive business-facing analytics layer.

------------------------------------------------------------------------

# 🎤 Interview Explanation

## 30-second answer

> I built an end-to-end FMCG Data Engineering pipeline processing
> approximately 1.1 million transactions. Kafka handles ingestion, HDFS
> stores raw data, PySpark performs ETL and enrichment, Parquet stores
> processed data, Hive provides analytics, Airflow orchestrates the
> workflow, and Streamlit provides the dashboard. I also implemented
> automated data-quality checks and containerized the infrastructure
> using Docker.

## 2-minute answer

> The project starts with FMCG transaction data plus product and store
> master data. I use Kafka as the ingestion layer and consumers move the
> transaction data into HDFS, which acts as the raw data lake. PySpark
> then reads the HDFS data, performs cleaning and type conversions,
> joins the transaction data with product and store master data, and
> creates analytics-ready sales data. The processed data is stored as
> partitioned Parquet.
>
> After ETL, a data-quality stage validates required fields, duplicate
> records, negative sales, and negative units. Hive provides the SQL
> analytics layer, where I calculate country, store, monthly, and
> product-level metrics. Airflow orchestrates the complete sequence from
> ingestion through ETL, quality validation, and Hive analytics.
> Finally, Streamlit queries the analytical layer and presents KPIs,
> charts, filters, and data-quality indicators.
>
> The final tested pipeline processed about 1.1 million records with
> zero missing values, zero duplicate records, zero negative sales, and
> zero negative units.

------------------------------------------------------------------------

# 🔮 Future Improvements

Possible production extensions:

-   AWS S3 data lake
-   Amazon MSK / managed Kafka
-   Cloud-based Spark
-   Incremental/CDC ingestion
-   Schema Registry
-   CI/CD with GitHub Actions
-   Automated unit/integration tests
-   Monitoring and alerting
-   Data lineage
-   Authentication and authorization
-   Automated data download/setup
-   Production secrets management
-   Cloud deployment

------------------------------------------------------------------------

# 🔐 GitHub Data Policy

The repository intentionally excludes:

``` text
Data/
airflow/logs/
*.db
.env
*.pyc
.vscode/
airflow/airflow.cfg
airflow/airflow-webserver.pid
```

This prevents large datasets, generated runtime files, local databases,
secrets, caches, and editor-specific files from being committed.

------------------------------------------------------------------------

# ⭐ Project Highlights

``` text
1.1M+       Transactions processed
102         Products
13          Stores
7           Countries
2021–2023   Sales period

0           Missing values
0           Duplicate records
0           Negative sales
0           Negative units

Kafka → HDFS → Spark → Parquet → Hive → Airflow → Streamlit
```

------------------------------------------------------------------------

## 👨‍💻 Author

**Attharva Umate**

Data Engineering / Big Data Analytics

**Skills demonstrated:** Python · SQL · Kafka · Hadoop HDFS · PySpark ·
Hive · Airflow · Docker · Streamlit · Git/GitHub

## 📜 License

This project is intended for educational, portfolio, and demonstration
purposes.
