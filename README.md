# FMCG Global Sales Data Engineering Pipeline

An end-to-end **Data Engineering project** for processing, transforming, analyzing, and visualizing FMCG (Fast-Moving Consumer Goods) sales data using **Apache Kafka, Hadoop HDFS, Apache Spark, PySpark, Hive, Apache Airflow, Docker, Python, Streamlit, and Plotly**.

The project processes approximately **1.1 million sales records** through a complete data engineering pipeline.

---

## 📌 Project Overview

This project demonstrates a complete Big Data Engineering pipeline:

    FMCG CSV Data
          ↓
    Apache Kafka
          ↓
    Hadoop HDFS
          ↓
    Apache Spark / PySpark
          ↓
    Parquet
          ↓
    Data Quality Checks
          ↓
    Apache Hive
          ↓
    Apache Airflow
          ↓
    Streamlit Dashboard

The objective is to build a scalable and automated pipeline that can:

- Ingest large volumes of FMCG sales data
- Store raw data in distributed storage
- Process data using Apache Spark
- Store processed data in Parquet
- Perform data-quality validation
- Analyze data using Hive SQL
- Orchestrate the pipeline using Airflow
- Visualize business insights using Streamlit

---

# 🏗️ Project Architecture

    ┌─────────────────────────┐
    │     FMCG CSV Data       │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │      Apache Kafka       │
    │     Data Ingestion      │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │       Hadoop HDFS       │
    │     Raw Data Storage    │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │    Apache Spark /       │
    │       PySpark ETL      │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │        Parquet          │
    │   Processed Data        │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │    Data Quality Check   │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │       Apache Hive       │
    │       Analytics         │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │      Apache Airflow     │
    │     Orchestration       │
    └────────────┬────────────┘
                 │
                 ▼
    ┌─────────────────────────┐
    │       Streamlit         │
    │       Dashboard         │
    └─────────────────────────┘

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | Programming and data processing |
| Apache Kafka | Data ingestion |
| Hadoop HDFS | Distributed storage |
| Apache Spark | Big Data processing |
| PySpark | Spark ETL development |
| Parquet | Processed columnar storage |
| Apache Hive | SQL analytics |
| Apache Airflow | Workflow orchestration |
| Docker | Containerization |
| Streamlit | Interactive dashboard |
| Plotly | Data visualization |
| Pandas | Data manipulation |
| Git | Version control |
| GitHub | Repository hosting |

---

# 📊 Dataset

The project uses FMCG sales transaction data.

### Dataset Information

- Approximately **1,100,000 transactions**
- **33 columns** in the original dataset
- **102 SKUs / products**
- **13 stores**
- Data covering **2021–2023**
- Data from **7 countries**

### Important Columns

    date
    store_id
    sku_id
    sku_name
    country
    units_sold
    net_sales

---

# 🔄 Data Engineering Pipeline

## 1. Data Ingestion – Apache Kafka

The raw FMCG sales dataset is published through Apache Kafka.

    CSV Dataset
         ↓
    Kafka Producer
         ↓
    Kafka Topic

Kafka acts as the ingestion layer for sales transactions.

The Kafka producer reads transaction records and publishes them to the configured Kafka topic.

---

# 2. Kafka → HDFS

A Kafka consumer receives transaction messages and stores the data in Hadoop HDFS.

Raw data location:

    /fmcg/raw/transactions

HDFS provides distributed storage for the raw transaction data.

The project uses HDFS to demonstrate Big Data distributed storage concepts.

---

# 3. Apache Spark / PySpark ETL

Apache Spark processes the raw transaction data.

The ETL process performs:

- Reading raw data
- Data type conversion
- Data transformation
- Dataset enrichment
- Column selection
- Sales dataset creation
- Product dataset creation
- Store dataset creation
- Writing processed data in Parquet format

Processed data locations:

    /fmcg/processed/sales
    /fmcg/processed/products
    /fmcg/processed/stores

---

# 4. Parquet Storage

Processed datasets are stored in **Parquet** format.

Parquet is a columnar storage format that is well suited for analytical workloads.

The final sales dataset contains approximately:

    1,100,000 records

---

# 5. Data Quality Validation

A dedicated Spark data-quality job validates the processed sales data.

### Validation Results

| Validation | Result |
|---|---:|
| Total Records | 1,100,000 |
| Null Date Values | 0 |
| Null Store IDs | 0 |
| Null SKU IDs | 0 |
| Null Units Sold | 0 |
| Null Net Sales | 0 |
| Duplicate Records | 0 |
| Negative Net Sales | 0 |
| Negative Units Sold | 0 |

The data-quality process helps ensure that invalid data is detected before the analytical stage.

---

# 🐝 6. Apache Hive

The processed data is analyzed using Apache Hive.

### Hive Database

    fmcg

### Main Table

    sales

Hive is used for SQL-based analytical processing.

### Main Analytics

- Sales by country
- Units sold by country
- Top-selling products
- Monthly sales trends
- Product performance
- Country performance

---

## Example Hive Query

    SELECT
        country,
        ROUND(SUM(net_sales), 2) AS total_sales,
        SUM(units_sold) AS total_units
    FROM sales
    GROUP BY country
    ORDER BY total_sales DESC;

---

# 🌍 Country-Level Analysis

The pipeline provides sales analytics for:

- Italy
- Spain
- Germany
- Austria
- France
- Poland
- Netherlands

Example analytical structure:

    Country       Total Sales
    -------------------------
    Italy         ...
    Spain         ...
    Germany       ...
    Austria       ...
    France        ...
    Poland        ...
    Netherlands   ...

---

# 📅 Monthly Sales Analysis

The sales data covers:

    2021
    2022
    2023

Monthly sales are calculated using Hive.

Example query:

    SELECT
        YEAR(sale_date) AS sales_year,
        MONTH(sale_date) AS sales_month,
        ROUND(SUM(net_sales), 2) AS total_sales
    FROM sales
    GROUP BY
        YEAR(sale_date),
        MONTH(sale_date)
    ORDER BY
        sales_year,
        sales_month;

The result is used by the Streamlit dashboard to display the monthly sales trend.

---

# 🔄 7. Apache Airflow

Apache Airflow is used to orchestrate the complete data pipeline.

### Airflow DAG

    START
      ↓
    Kafka Ingestion
      ↓
    Spark ETL
      ↓
    Data Quality Check
      ↓
    Hive Analytics
      ↓
    END

### Main Airflow Tasks

### `kafka_ingestion`

Consumes Kafka messages and stores transaction data in HDFS.

### `spark_etl`

Runs the PySpark ETL pipeline.

### `data_quality_check`

Validates the processed data.

### `hive_analytics`

Runs Hive analytical queries.

Airflow provides workflow scheduling, dependency management, monitoring, and pipeline orchestration.

---

# 📈 8. Streamlit Dashboard

An interactive dashboard was developed using Streamlit.

### Dashboard Features

- Total Sales
- Total Units Sold
- Number of Countries
- Sales by Country
- Units Sold Analysis
- Top 10 Products
- Monthly Sales Trend
- Year-based analysis/filtering

### Dashboard Technologies

    Streamlit
    Pandas
    Plotly
    Python
    Hive

The dashboard retrieves analytical data from Hive and presents it using interactive visualizations.

---

# 🐳 Docker Environment

The project uses Docker to run the Big Data infrastructure.

Main services include:

    fmcg-kafka
    fmcg-zookeeper
    fmcg-namenode
    fmcg-datanode
    fmcg-spark
    fmcg-hive
    fmcg-airflow
    fmcg-airflow-scheduler

Docker provides an isolated and reproducible environment for the project.

---

# 📁 Project Structure

    C-FMCG-Data-Engineering-/
    │
    ├── Data/
    │   └── transactions/
    │
    ├── kafka/
    │   ├── producer.py
    │   ├── consumer.py
    │   ├── consumer_hdfs.py
    │   └── Dockerfile
    │
    ├── spark/
    │   └── jobs/
    │       ├── fmcg_etl.py
    │       └── data_quality_check.py
    │
    ├── airflow/
    │   └── dags/
    │       └── fmcg_sales_pipeline.py
    │
    ├── dashboard/
    │   └── app.py
    │
    ├── docker/
    │   ├── docker-compose.yml
    │   └── hadoop-conf/
    │
    ├── create_sales.sql
    ├── data_quality_check.py
    ├── README.md
    └── .gitignore

---

# 🚀 Installation and Setup

## Prerequisites

Install the following:

- Docker Desktop
- Python 3.x
- Git
- VS Code

---

# 1. Clone Repository

    git clone https://github.com/Attharva10/C-FMCG-Data-Engineering-.git

Go to the project directory:

    cd C-FMCG-Data-Engineering-

---

# 2. Start Docker Services

Run:

    docker compose -f .\docker\docker-compose.yml up -d

Check running containers:

    docker ps

---

# 3. Verify HDFS

Check raw transaction data:

    docker exec fmcg-namenode hdfs dfs -ls /fmcg/raw/transactions

Check processed sales data:

    docker exec fmcg-namenode hdfs dfs -ls /fmcg/processed/sales

---

# 4. Run Spark ETL

    docker exec fmcg-spark /opt/spark/bin/spark-submit /opt/spark/jobs/fmcg_etl.py

---

# 5. Run Data Quality Check

    docker exec fmcg-spark /opt/spark/bin/spark-submit /opt/spark/jobs/data_quality_check.py

Expected validation:

    Total records: 1,100,000
    Duplicate records: 0
    Negative net sales: 0
    Negative units sold: 0

---

# 6. Hive

HiveServer2 uses port:

    10000

Start Beeline:

    docker exec -it fmcg-hive beeline

Hive database:

    fmcg

Main table:

    sales

---

# 7. Airflow

Open the Airflow web interface provided by the Docker environment.

Trigger the DAG:

    fmcg_sales_pipeline

Expected workflow:

    kafka_ingestion
            ↓
    spark_etl
            ↓
    data_quality_check
            ↓
    hive_analytics

All tasks should complete successfully.

---

# 8. Run Streamlit Dashboard

Install required Python packages:

    python -m pip install streamlit pandas plotly

Run the dashboard:

    python -m streamlit run .\dashboard\app.py

Open the local Streamlit URL displayed in the terminal.

---

# 🔍 Project Validation

The complete project was validated across the main pipeline components.

    Kafka              ✅
    HDFS               ✅
    Spark ETL          ✅
    Parquet            ✅
    Data Quality       ✅
    Hive               ✅
    Airflow            ✅
    Streamlit          ✅
    GitHub             ✅

### Data Quality Results

    Records Processed:      1,100,000
    Null Important Values:  0
    Duplicate Records:      0
    Negative Sales:         0
    Negative Units:         0

---

# 💡 Business Questions

This project can answer business questions such as:

1. Which countries generated the highest sales?
2. How many units were sold by country?
3. Which products generated the highest revenue?
4. What are the monthly sales trends?
5. Which SKUs are performing strongly?
6. How can FMCG sales data be processed at scale?
7. How can the complete pipeline be automated using Airflow?

---

# 🎯 Learning Outcomes

This project demonstrates practical experience with:

- Data ingestion
- Apache Kafka
- Hadoop HDFS
- Distributed storage
- Apache Spark
- PySpark
- ETL pipelines
- Parquet
- Data quality validation
- Apache Hive
- SQL analytics
- Apache Airflow
- Docker
- Streamlit
- Plotly
- Pandas
- Git
- GitHub

---

# 🔮 Future Enhancements

## 1. Real-Time Streaming

Future architecture:

    Kafka
       ↓
    Spark Structured Streaming
       ↓
    Data Lake
       ↓
    Hive
       ↓
    Dashboard

---

## 2. AWS Cloud Deployment

Potential future architecture:

    Kafka / MSK
         ↓
    Amazon S3
         ↓
    EMR / Databricks
         ↓
    AWS Glue / Athena
         ↓
    Dashboard

---

## 3. Incremental Processing

Instead of processing the complete dataset every time, process only newly arrived transactions.

Example:

    Existing Data
         +
    New Transactions
         ↓
    Incremental ETL
         ↓
    Updated Data

---

## 4. Advanced Data Quality

Future validation can include:

- Schema validation
- Invalid date detection
- Invalid SKU detection
- Missing store validation
- Unexpected country detection
- Outlier detection
- Data anomaly detection

---

## 5. Monitoring and Alerts

Future monitoring can include:

- Airflow task failures
- Kafka ingestion failures
- Spark failures
- Data-quality failures
- Pipeline execution time
- Storage monitoring

---

## 6. Machine Learning

Future ML functionality could include sales forecasting.

    Historical Sales
          ↓
    Feature Engineering
          ↓
    Machine Learning Model
          ↓
    Sales Forecast
          ↓
    Dashboard

Possible use cases:

- Sales forecasting
- Demand prediction
- Product demand analysis
- Store demand prediction

---

## 7. CI/CD

GitHub Actions can be added for:

- Automated testing
- Code validation
- Docker image builds
- Deployment automation

---

# 👨‍💻 Interview Project Description

> Built an end-to-end FMCG sales Data Engineering pipeline processing approximately 1.1 million records using Apache Kafka, Hadoop HDFS, PySpark, Hive, and Apache Airflow. Implemented distributed ETL, Parquet-based storage, automated data-quality validation, Hive analytics, Docker-based infrastructure, and an interactive Streamlit dashboard for business insights.

---

# ⭐ Project Highlights

- **1.1M+ records processed**
- Kafka-based data ingestion
- Hadoop HDFS distributed storage
- PySpark ETL pipeline
- Parquet data storage
- Automated data-quality validation
- Hive analytical processing
- Airflow workflow orchestration
- Docker containerization
- Streamlit dashboard
- Plotly visualizations
- Git/GitHub version control

---

# 📌 Project Status

    Data Ingestion       ✅ Complete
    Kafka Pipeline       ✅ Complete
    HDFS Storage         ✅ Complete
    Spark ETL            ✅ Complete
    Parquet Processing   ✅ Complete
    Data Quality         ✅ Complete
    Hive Analytics       ✅ Complete
    Airflow Pipeline     ✅ Complete
    Streamlit Dashboard  ✅ Complete
    GitHub Repository    ✅ Complete
    Documentation        ✅ Complete

---

# 📜 License

This project is intended for educational, learning, and portfolio purposes.

---

## 👤 Author

**Attharva10**

GitHub:

https://github.com/Attharva10
