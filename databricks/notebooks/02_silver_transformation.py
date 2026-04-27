from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.config import *

spark = SparkSession.builder.appName("ABM Silver Production").getOrCreate()

# ----------------------------
# Load Bronze Data
# ----------------------------
demandbase = spark.read.format("delta").load(BRONZE_DELTA + "demandbase")
salesforce = spark.read.format("delta").load(BRONZE_DELTA + "salesforce")
linkedin = spark.read.format("delta").load(BRONZE_DELTA + "linkedin")
ga4 = spark.read.format("delta").load(BRONZE_DELTA + "ga4")

# ----------------------------
# Standardize Columns
# ----------------------------
demandbase = demandbase.toDF(*[c.lower() for c in demandbase.columns])
salesforce = salesforce.toDF(*[c.lower() for c in salesforce.columns])
linkedin = linkedin.toDF(*[c.lower() for c in linkedin.columns])
ga4 = ga4.toDF(*[c.lower() for c in ga4.columns])

# ----------------------------
# Cleaning
# ----------------------------
demandbase = demandbase.withColumn("company_name", trim(col("company_name")))
salesforce = salesforce.withColumn("stage", trim(col("stage")))

# Handle nulls
demandbase = demandbase.fillna({"intent_score": 0, "industry": "unknown"})
salesforce = salesforce.fillna({"opportunity_value": 0, "stage": "unknown"})
linkedin = linkedin.fillna({"ad_clicks": 0, "impressions": 0})
ga4 = ga4.fillna({"sessions": 0, "users": 0, "conversions": 0, "bounce_rate": 0.0})

# Type casting
salesforce = salesforce.withColumn("opportunity_value", col("opportunity_value").cast("double"))
demandbase = demandbase.withColumn("intent_score", col("intent_score").cast("int"))

# ----------------------------
# Deduplication
# ----------------------------
window_spec = Window.partitionBy("account_id").orderBy(col("ingestion_time").desc())

demandbase = demandbase.withColumn("rn", row_number().over(window_spec)) \
                       .filter(col("rn") == 1).drop("rn")

# ----------------------------
# Feature Engineering
# ----------------------------
ga4 = ga4.withColumn(
    "conversion_rate",
    when(col("sessions") > 0, col("conversions") / col("sessions")).otherwise(0)
)

linkedin = linkedin.withColumn(
    "ctr",
    when(col("impressions") > 0, col("ad_clicks") / col("impressions")).otherwise(0)
)

demandbase = demandbase.withColumn(
    "intent_score_normalized",
    col("intent_score") / 100
)

# ----------------------------
# Join Data
# ----------------------------
silver_df = demandbase \
    .join(salesforce, "account_id", "left") \
    .join(linkedin, "account_id", "left") \
    .join(ga4, "account_id", "left")

# ----------------------------
# Business Features
# ----------------------------
silver_df = silver_df.withColumn(
    "engagement_score",
    (col("intent_score_normalized") * 0.4) +
    (col("ctr") * 0.3) +
    (col("conversion_rate") * 0.3)
)

silver_df = silver_df.withColumn(
    "revenue_bucket",
    when(col("opportunity_value") > 50000, "High")
    .when(col("opportunity_value") > 20000, "Medium")
    .otherwise("Low")
)

# ----------------------------
# Audit + Partition
# ----------------------------
silver_df = silver_df.withColumn("ingestion_date", current_date())
silver_df = silver_df.withColumn("processed_timestamp", current_timestamp())

# ----------------------------
# MERGE (Incremental)
# ----------------------------
silver_path = SILVER_DELTA + "abm"

if DeltaTable.isDeltaTable(spark, silver_path):
    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("t").merge(
        silver_df.alias("s"),
        "t.account_id = s.account_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    silver_df.write.format("delta") \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .save(silver_path)

# ----------------------------
# Optimization
# ----------------------------
spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (account_id)")

print("✅ Silver layer complete (transform + incremental + optimized)")
