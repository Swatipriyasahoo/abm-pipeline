# Databricks - ABM Data Pipeline

## 📌 Overview

This module implements a production-grade data processing pipeline using Databricks and Delta Lake for an Account-Based Marketing (ABM) use case.

It processes raw data ingested via Azure Data Factory (ADF) from multiple sources and transforms it into analytics-ready datasets using the Medallion Architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture

ADF → ADLS (Raw/Bronze) → Databricks → Delta Lake (Bronze/Silver/Gold) → SQL Analytics / BI

---

## 📊 Data Sources

* Demandbase (Intent Data)
* LinkedIn Ads
* Salesforce CRM
* GA4 (via BigQuery)

---

## 🧱 Medallion Architecture

### 🥉 Bronze Layer

* Raw data ingestion from ADLS (written by ADF)
* Incremental loading using Delta Lake MERGE
* Adds ingestion timestamps

### 🥈 Silver Layer

* Data cleaning and standardization
* Null handling and type casting
* Deduplication using window functions
* Feature engineering:

  * Conversion rate
  * CTR (Click-through rate)
  * Engagement score
* Multi-source joins (Demandbase + Salesforce + LinkedIn + GA4)
* Incremental processing using MERGE
* Partitioning by ingestion_date
* Optimization using ZORDER

### 🥇 Gold Layer

* Business-ready ABM dataset
* Advanced scoring model combining:

  * Intent signals
  * Engagement metrics
  * Conversion performance
  * Revenue contribution
* Account segmentation:

  * Tiering (High / Medium / Low)
  * Deciles
  * Ranking
* Business recommendations:

  * Sales actions
  * Marketing channel focus
* Production features:

  * Change detection (hash-based)
  * Surrogate keys
  * Late-arriving data handling
  * Conditional MERGE updates
* Performance optimization:

  * ZORDER
  * VACUUM

---

## ⚙️ Key Features

### 🔁 Incremental Processing

* Implemented using Delta Lake MERGE
* Ensures idempotent and scalable pipelines

### 🧹 Data Quality Handling

* Null handling and default values
* Negative value correction
* Deduplication logic
* Schema standardization

### ⚡ Performance Optimization

* Partitioning by ingestion_date
* ZORDER on account_id
* VACUUM for storage cleanup

### 🧠 Business Logic

* ABM scoring model (weighted)
* Engagement and conversion metrics
* Revenue normalization
* Actionable insights for sales & marketing

### 🔍 Change Data Handling

* Hash-based change detection
* Updates only when data changes
* Prevents unnecessary rewrites

---

## 📁 Project Structure

databricks/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_abm_model.py
│
├── utils/
│   └── config.py
│
├── sql/
│   └── analysis.sql
│
└── README.md

---

## 🔄 Orchestration

The pipeline is orchestrated using Azure Data Factory:

1. ADF ingests data from APIs into ADLS (Bronze layer)
2. ADF triggers Databricks notebooks
3. Databricks processes data through:

   * Bronze → Silver → Gold
4. Final data is available for analytics and reporting

---

## 📊 Output

* Unified account-level dataset
* ABM scoring and segmentation
* Sales prioritization insights
* Marketing performance metrics
* Ready for Power BI / dashboards

---

## 🚀 How to Run

1. Ensure data is available in ADLS (via ADF pipelines)
2. Run Databricks notebooks in order:

   * 01_bronze_ingestion.py
   * 02_silver_transformation.py
   * 03_gold_abm_model.py
3. Query results using SQL (analysis.sql)

---

## 🎯 Use Cases

* Sales account prioritization
* Marketing campaign optimization
* Conversion funnel analysis
* Revenue forecasting
* Customer segmentation

---

## 💡 Future Enhancements

* Streaming ingestion (Structured Streaming)
* CI/CD integration for deployment
* Data quality framework (Great Expectations)
* Monitoring and alerting
* Cost optimization strategies

---

## 🧠 Key Learnings

* End-to-end data pipeline design
* Delta Lake incremental processing
* Medallion architecture implementation
* Business-driven data modeling
* Performance optimization techniques
