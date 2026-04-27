# How to Run the Pipeline

## 📌 Prerequisites

* Azure Data Factory configured
* ADLS storage available
* Databricks workspace set up

---

## 🚀 Execution Steps

### 1. Run ADF Pipelines

* Execute ingestion pipelines for:

  * Demandbase
  * LinkedIn
  * Salesforce
  * GA4

---

### 2. Run Databricks Notebooks

Execute in order:

1. 01_bronze_ingestion.py
2. 02_silver_transformation.py
3. 03_gold_abm_model.py

---

### 3. Query Data

* Use SQL queries from `sql/analysis.sql`
* Connect to BI tools (Power BI)

---

## 🔁 Pipeline Behavior

* Incremental loads using MERGE
* Updates only changed data
* Optimized for performance

---

## ⚠️ Notes

* Ensure correct paths in config.py
* Verify ADLS access before running

