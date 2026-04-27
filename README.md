# ABM Data Pipeline (End-to-End Data Engineering Project)

## 📌 Overview

This project demonstrates a **production-grade end-to-end data pipeline** for an Account-Based Marketing (ABM) use case.

It integrates multiple marketing and CRM data sources, processes them using modern data engineering practices, and delivers business-ready insights for sales and marketing teams.

---

## 🏗️ Architecture

ADF → ADLS → Databricks → Delta Lake → SQL Analytics / BI

---

## 📊 Data Sources

* Demandbase (Intent Data)
* LinkedIn Ads
* Salesforce CRM
* GA4 (via BigQuery)

---

## ⚙️ Tech Stack

* Azure Data Factory (ADF)
* Azure Data Lake Storage (ADLS Gen2)
* Databricks (PySpark)
* Delta Lake
* SQL

---

## 🔄 Pipeline Flow

### 1. Data Ingestion (ADF)

* Data is ingested from APIs:

  * Demandbase
  * LinkedIn
  * Salesforce
  * GA4 (BigQuery)
* ADF pipelines load raw data into ADLS (Bronze layer)

---

### 2. Data Processing (Databricks)

#### 🥉 Bronze Layer

* Raw ingestion from ADLS
* Incremental loads using MERGE
* Audit columns added

#### 🥈 Silver Layer

* Data cleaning and standardization
* Null handling and type casting
* Deduplication using window functions
* Feature engineering:

  * Conversion rate
  * CTR (Click-through rate)
  * Engagement score
* Multi-source joins
* Incremental MERGE logic
* Partitioning by ingestion_date
* Optimization using ZORDER

#### 🥇 Gold Layer

* Business-ready ABM dataset
* Advanced scoring model
* Account segmentation:

  * Tiering
  * Ranking
  * Deciles
* Sales & marketing recommendations
* Change data handling (hash-based)
* Surrogate keys
* Late-arriving data handling
* Performance optimization (ZORDER, VACUUM)

---

## 📁 Project Structure

adf/ → ADF pipelines (API ingestion)
databricks/ → PySpark pipelines (Bronze/Silver/Gold)

---

## 🔁 Key Features

### ✅ Incremental Processing

* Implemented using Delta Lake MERGE
* Ensures idempotent and scalable pipelines

### ✅ Data Quality Handling

* Null handling
* Deduplication
* Schema standardization
* Invalid data correction

### ✅ Performance Optimization

* Partitioning
* ZORDER
* VACUUM

### ✅ Business Logic

* ABM scoring model
* Customer segmentation
* Actionable insights

### ✅ Change Data Handling

* Hash-based change detection
* Conditional updates

---

## 📊 Output

* Unified account-level dataset
* ABM scoring and prioritization
* Marketing performance insights
* Sales action recommendations
* Ready for dashboards (Power BI)

---

## 🚀 How to Run

1. Run ADF pipelines to ingest data into ADLS
2. Execute Databricks notebooks in order:

   * Bronze ingestion
   * Silver transformation
   * Gold model
3. Query using SQL for analytics

---

## 🎯 Use Cases

* Sales account prioritization
* Marketing campaign optimization
* Conversion analysis
* Revenue forecasting
* Customer segmentation

---

## 🧠 What This Project Demonstrates

* End-to-end data pipeline design
* Medallion architecture (Bronze/Silver/Gold)
* Incremental data processing
* Data modeling for business use cases
* Performance optimization techniques
* Real-world data engineering practices

---

## 💡 Future Enhancements

* Streaming pipelines (Structured Streaming)
* CI/CD integration
* Data quality framework
* Monitoring and alerting
* Cost optimization



Built as a portfolio project to demonstrate real-world data engineering capabilities.
