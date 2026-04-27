from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.config import *

spark = SparkSession.builder.appName("ABM Gold Production Advanced").getOrCreate()

# ----------------------------
# Load Silver
# ----------------------------
silver = spark.read.format("delta").load(SILVER_DELTA + "abm")

# ----------------------------
# DATA QUALITY CHECKS
# ----------------------------

# Drop invalid records
silver = silver.filter(col("account_id").isNotNull())

# Remove negative values (bad data)
silver = silver.withColumn(
    "opportunity_value",
    when(col("opportunity_value") < 0, 0).otherwise(col("opportunity_value"))
)

# ----------------------------
# HANDLE LATE ARRIVING DATA
# ----------------------------

# Deduplicate using latest processed_timestamp
window_spec_latest = Window.partitionBy("account_id").orderBy(col("processed_timestamp").desc())

silver = silver.withColumn("rn", row_number().over(window_spec_latest)) \
               .filter(col("rn") == 1) \
               .drop("rn")

# ----------------------------
# NORMALIZE REVENUE
# ----------------------------
max_rev = silver.agg(max("opportunity_value")).collect()[0][0]

silver = silver.withColumn(
    "normalized_revenue",
    when(col("opportunity_value") > 0, col("opportunity_value") / max_rev).otherwise(0)
)

# ----------------------------
# ABM SCORE
# ----------------------------
gold_df = silver.withColumn(
    "abm_score",
    round(
        (col("intent_score_normalized") * 0.25) +
        (col("engagement_score") * 0.30) +
        (col("conversion_rate") * 0.20) +
        (col("normalized_revenue") * 0.25),
        4
    )
)

# ----------------------------
# SEGMENTATION
# ----------------------------
gold_df = gold_df.withColumn(
    "account_tier",
    when(col("abm_score") >= 0.75, "Tier 1 - High Priority")
    .when(col("abm_score") >= 0.5, "Tier 2 - Medium Priority")
    .otherwise("Tier 3 - Low Priority")
)

window_spec = Window.orderBy(desc("abm_score"))

gold_df = gold_df.withColumn("account_rank", row_number().over(window_spec))
gold_df = gold_df.withColumn("decile", ntile(10).over(window_spec))

# ----------------------------
# BUSINESS RECOMMENDATIONS
# ----------------------------
gold_df = gold_df.withColumn(
    "recommended_action",
    when(col("account_tier") == "Tier 1 - High Priority", "Immediate Sales Outreach")
    .when(col("account_tier") == "Tier 2 - Medium Priority", "Nurture Campaign")
    .otherwise("Low Touch")
)

gold_df = gold_df.withColumn(
    "channel_focus",
    when(col("conversion_rate") > 0.2, "Retargeting")
    .when(col("ctr") > 0.1, "LinkedIn Ads")
    .otherwise("Email")
)

# ----------------------------
# SURROGATE KEY (WAREHOUSE STYLE)
# ----------------------------
gold_df = gold_df.withColumn(
    "account_sk",
    sha2(concat_ws("||", col("account_id"), col("company_name")), 256)
)

# ----------------------------
# CHANGE DETECTION (SCD-lite)
# ----------------------------
gold_df = gold_df.withColumn(
    "hash_diff",
    sha2(concat_ws("||",
        col("abm_score"),
        col("account_tier"),
        col("engagement_score"),
        col("normalized_revenue")
    ), 256)
)

# ----------------------------
# AUDIT COLUMNS
# ----------------------------
gold_df = gold_df.withColumn("ingestion_date", current_date())
gold_df = gold_df.withColumn("processed_timestamp", current_timestamp())
gold_df = gold_df.withColumn("is_active", lit(1))

# ----------------------------
# TARGET PATH
# ----------------------------
gold_path = GOLD_DELTA + "abm"

# ----------------------------
# MERGE WITH CHANGE DETECTION
# ----------------------------
if DeltaTable.isDeltaTable(spark, gold_path):
    delta_table = DeltaTable.forPath(spark, gold_path)

    delta_table.alias("t").merge(
        gold_df.alias("s"),
        "t.account_id = s.account_id"
    ).whenMatchedUpdate(
        condition="t.hash_diff != s.hash_diff",
        set={
            "abm_score": "s.abm_score",
            "account_tier": "s.account_tier",
            "engagement_score": "s.engagement_score",
            "normalized_revenue": "s.normalized_revenue",
            "recommended_action": "s.recommended_action",
            "channel_focus": "s.channel_focus",
            "processed_timestamp": "s.processed_timestamp",
            "hash_diff": "s.hash_diff"
        }
    ).whenNotMatchedInsertAll() \
     .execute()
else:
    gold_df.write.format("delta") \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .save(gold_path)

# ----------------------------
# OPTIMIZATION
# ----------------------------
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (account_id)")

# ----------------------------
# VACUUM (cleanup old files)
# ----------------------------
spark.sql(f"VACUUM delta.`{gold_path}` RETAIN 168 HOURS")

# ----------------------------
# METRICS LOGGING
# ----------------------------
total_records = gold_df.count()
tier1_count = gold_df.filter(col("account_tier") == "Tier 1 - High Priority").count()

print(f"Total Records Processed: {total_records}")
print(f"Tier 1 Accounts: {tier1_count}")

print("✅ Gold layer complete (fully production-grade)")
