# Pipeline Flow

## 📌 Overview

This document explains the step-by-step execution of the data pipeline.

---

## 🔄 End-to-End Flow

### Step 1: Data Ingestion (ADF)

* APIs are called via ADF pipelines
* Data is stored in ADLS in raw format

---

### Step 2: Bronze Layer (Databricks)

* Reads raw data from ADLS
* Adds ingestion timestamps
* Performs incremental load using MERGE

---

### Step 3: Silver Layer

* Cleans data (null handling, type casting)
* Deduplicates records
* Joins multiple sources
* Creates derived features:

  * conversion_rate
  * ctr
  * engagement_score

---

### Step 4: Gold Layer

* Calculates ABM score
* Segments accounts (tiering, deciles)
* Adds recommendations
* Performs incremental MERGE
* Optimizes using ZORDER

---

### Step 5: Analytics

* SQL queries used for reporting
* Data available for dashboards

---

## ⚙️ Key Techniques Used

* Delta Lake MERGE (incremental)
* Window functions (deduplication)
* Partitioning (ingestion_date)
* Optimization (ZORDER)

