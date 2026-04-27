from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, lit, regexp_replace
from pyspark.sql.functions import current_timestamp, coalesce, round
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from utils.config import *

spark = SparkSession.builder.appName("ABM Silver Layer").getOrCreate()

# ----------------------------
# Load Bronze Data
# ----------------------------
demandbase = spark.read.format("delta").load(BRONZE_DELTA + "demandbase")
salesforce = spark.read.format("delta").load(BRONZE_DELTA + "salesforce")
linkedin = spark.read.format("delta").load(BRONZE_DELTA + "linkedin")
ga4 = spark.read.format("delta").load(BRONZE_DELTA + "ga4")

# ----------------------------
# 1. Standardize Column Names
# ----------------------------
demandbase = demandbase.toDF(*[c.lower() for c in demandbase.columns])
salesforce = salesforce.toDF(*[c.lower() for c in salesforce.columns])
linkedin = linkedin.toDF(*[c.lower() for c in linkedin.columns])
ga4 = ga4.toDF(*[c.lower() for c in ga4.columns])

# ----------------------------
# 2. Trim & Clean Strings
# ----------------------------
demandbase = demandbase.withColumn("company_name", trim(col("company_name")))
salesforce = salesforce.withColumn("stage", trim(col("stage")))

# ----------------------------
# 3. Handle Missing Values
# ----------------------------
demandbase = demandbase.fillna({
    "intent_score": 0,
    "industry": "unknown"
})

salesforce = salesforce.fillna({
    "opportunity_value": 0,
    "stage": "unknown"
})

linkedin = linkedin.fillna({
    "ad_clicks": 0,
    "impressions": 0
})

ga4 = ga4.fillna({
    "sessions": 0,
    "users": 0,
    "conversions": 0,
    "bounce_rate": 0.0
})

# ----------------------------
# 4. Data Type Casting
# ----------------------------
salesforce = salesforce.withColumn("opportunity_value", col("opportunity_value").cast("double"))
demandbase = demandbase.withColumn("intent_score", col("intent_score").cast("int"))

# ----------------------------
# 5. Remove Duplicates (latest record logic)
# ----------------------------
window_spec = Window.partitionBy("account_id").orderBy(col("intent_score").desc())

demandbase = demandbase.withColumn("rn", row_number().over(window_spec)) \
                       .filter(col("rn") == 1) \
                       .drop("rn")

# ----------------------------
# 6. Feature Engineering
# ----------------------------

# Conversion rate
ga4 = ga4.withColumn(
    "conversion_rate",
    when(col("sessions") > 0, col("conversions") / col("sessions")).otherwise(0)
)

# CTR (click-through rate)
linkedin = linkedin.withColumn(
    "ctr",
    when(col("impressions") > 0, col("ad_clicks") / col("impressions")).otherwise(0)
)

# Normalize intent score (0–1 scale)
demandbase = demandbase.withColumn(
    "intent_score_normalized",
    col("intent_score") / 100
)

# ----------------------------
# 7. Join All Sources
# ----------------------------
silver_df = demandbase \
    .join(salesforce, "account_id", "left") \
    .join(linkedin, "account_id", "left") \
    .join(ga4, "account_id", "left")

# ----------------------------
# 8. Derived Business Columns
# ----------------------------

# Engagement score
silver_df = silver_df.withColumn(
    "engagement_score",
    (col("intent_score_normalized") * 0.4) +
    (col("ctr") * 0.3) +
    (col("conversion_rate") * 0.3)
)

# Revenue bucket
silver_df = silver_df.withColumn(
    "revenue_bucket",
    when(col("opportunity_value") > 50000, "High")
    .when(col("opportunity_value") > 20000, "Medium")
    .otherwise("Low")
)

# ----------------------------
# 9. Add Audit Columns
# ----------------------------
silver_df = silver_df.withColumn("ingestion_timestamp", current_timestamp())

# ----------------------------
# 10. Final Selection (clean schema)
# ----------------------------
silver_df = silver_df.select(
    "account_id",
    "company_name",
    "industry",
    "intent_score",
    "intent_score_normalized",
    "opportunity_value",
    "stage",
    "ad_clicks",
    "impressions",
    "ctr",
    "sessions",
    "users",
    "conversions",
    "conversion_rate",
    "engagement_score",
    "revenue_bucket",
    "ingestion_timestamp"
)

# ----------------------------
# Write Silver Layer
# ----------------------------
silver_df.write.format("delta").mode("overwrite").save(SILVER_DELTA + "abm")

print("✅ Silver layer created with advanced transformations")
