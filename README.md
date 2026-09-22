@'
# FMCG Global Sales Data Engineering Pipeline

## 📌 Project Overview

A scalable FMCG Global Sales Data Engineering pipeline that processes 1.1 million sales transactions using Kafka, HDFS, Apache Spark, Hive, and Airflow.

The pipeline performs data ingestion, transformation, enrichment, storage, analytics, and data quality validation.

## 🛠️ Technologies Used

- Apache Kafka – Data ingestion
- HDFS – Distributed data storage
- Apache Spark / PySpark – ETL processing
- Apache Hive – SQL analytics
- Apache Airflow – Workflow orchestration
- Docker – Containerization
- PowerShell – Project execution

## 🏗️ Architecture

Raw CSV Data
    ↓
Apache Kafka
    ↓
HDFS
    ↓
Apache Spark / PySpark
    ↓
Parquet Data
    ↓
Apache Hive
    ↓
SQL Analytics

Apache Airflow orchestrates the pipeline.

## 📊 Dataset

- Sales transactions: 1,100,000 records
- Product master data
- Store master data
- Multiple countries and sales channels

## 🔄 Pipeline Workflow

1. Read raw FMCG sales data.
2. Ingest transactions through Kafka.
3. Store raw data in HDFS.
4. Clean and transform data using PySpark.
5. Enrich transactions using product and store master data.
6. Store processed data in partitioned Parquet format.
7. Create Hive tables and partitions.
8. Execute country-level sales analytics.
9. Run data quality checks.
10. Orchestrate the workflow using Airflow.

## ✅ Data Quality Results

| Check | Result |
|---|---|
| Total records | 1,100,000 |
| NULL values in important columns | 0 |
| Duplicate records | 0 |
| Negative net sales | 0 |
| Negative units sold | 0 |
| Airflow pipeline | Success |

## 📁 Project Structure

```text
FMCG-Data-Engineering/
├── airflow/
├── Data/
├── docker/
├── kafka/
├── Scripts/
├── spark/
├── create_sales.sql
├── data_quality_check.py
├── docker-compose.yml
└── README.md
