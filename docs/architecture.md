# Architecture Overview

## 📌 Overview

This project implements an end-to-end data pipeline for Account-Based Marketing (ABM) using Azure and Databricks.

The architecture follows a modern lakehouse approach using ADLS and Delta Lake.

---

## 🏗️ High-Level Architecture

ADF → ADLS → Databricks → Delta Lake → SQL / BI

---

## 🔄 Flow Explanation

1. **Data Ingestion (ADF)**

   * APIs: Demandbase, LinkedIn, Salesforce, GA4
   * Data is ingested using Azure Data Factory pipelines

2. **Storage (ADLS)**

   * Raw data stored in Azure Data Lake (Bronze layer)
   * Format: CSV / Parquet

3. **Processing (Databricks)**

   * Bronze → Raw ingestion with incremental loads
   * Silver → Data cleaning, joins, transformations
   * Gold → Business logic and ABM scoring

4. **Analytics**

   * SQL queries used for reporting
   * Data can be consumed in Power BI

---

## 🧱 Key Design Principles

* Medallion Architecture (Bronze → Silver → Gold)
* Incremental processing using Delta MERGE
* Partitioning for performance
* ZORDER optimization
* Separation of ingestion and processing

---

## 🖼️ Diagram

![Architecture](../architecture/abm_pipeline_architecture.png)

