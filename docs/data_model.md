# Data Model

## 📌 Overview

This document describes the schema and structure of the Gold layer dataset used for ABM analytics.

---

## 🥇 Gold Table: `abm`

### Primary Key

* account_id

---

## 📊 Columns

| Column Name         | Description                           |
| ------------------- | ------------------------------------- |
| account_id          | Unique identifier for account         |
| company_name        | Company name                          |
| industry            | Industry type                         |
| abm_score           | Final ABM score                       |
| account_tier        | Tier classification (High/Medium/Low) |
| account_rank        | Rank based on score                   |
| decile              | Segmentation bucket (1–10)            |
| engagement_score    | Engagement metric                     |
| conversion_rate     | Conversion ratio                      |
| normalized_revenue  | Revenue scaled between 0–1            |
| recommended_action  | Suggested sales action                |
| channel_focus       | Marketing channel recommendation      |
| ingestion_date      | Partition column                      |
| processed_timestamp | Processing timestamp                  |

---

## 🧠 Business Logic

* ABM score is calculated using weighted metrics:

  * Intent score
  * Engagement score
  * Conversion rate
  * Revenue

---

## 🔁 Data Flow

Bronze → Silver → Gold transformation pipeline ensures:

* Clean data
* Enriched features
* Business-ready dataset

