from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, ntile, desc
from pyspark.sql.window import Window
from utils.config import *

spark = SparkSession.builder.appName("ABM Gold Layer").getOrCreate()

# ----------------------------
# Load Silver Data
# ----------------------------
silver = spark.read.format("delta").load(SILVER_DELTA + "abm")

# ----------------------------
# 1. Normalize Key Metrics
# ----------------------------

# Normalize opportunity value (scale 0–1)
max_revenue = silver.agg({"opportunity_value": "max"}).collect()[0][0]

silver = silver.withColumn(
    "normalized_revenue",
    when(col("opportunity_value") > 0, col("opportunity_value") / max_revenue).otherwise(0)
)

# ----------------------------
# 2. Final ABM Score (weighted model)
# ----------------------------

gold = silver.withColumn(
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
# 3. Account Segmentation (Tiering)
# ----------------------------

gold = gold.withColumn(
    "account_tier",
    when(col("abm_score") >= 0.75, "Tier 1 - High Priority")
    .when(col("abm_score") >= 0.5, "Tier 2 - Medium Priority")
    .otherwise("Tier 3 - Low Priority")
)

# ----------------------------
# 4. Ranking Accounts (within dataset)
# ----------------------------

window_spec = Window.orderBy(desc("abm_score"))

gold = gold.withColumn("account_rank", row_number().over(window_spec))

# ----------------------------
# 5. Decile Segmentation (Top 10%, etc.)
# ----------------------------

gold = gold.withColumn(
    "decile",
    ntile(10).over(window_spec)
)

# ----------------------------
# 6. Sales Action Recommendation
# ----------------------------

gold = gold.withColumn(
    "recommended_action",
    when(col("account_tier") == "Tier 1 - High Priority", "Immediate Sales Outreach")
    .when(col("account_tier") == "Tier 2 - Medium Priority", "Nurture Campaign")
    .otherwise("Low Touch / Awareness Campaign")
)

# ----------------------------
# 7. Marketing Channel Suggestion
# ----------------------------

gold = gold.withColumn(
    "channel_focus",
    when(col("conversion_rate") > 0.2, "Retargeting")
    .when(col("ctr") > 0.1, "LinkedIn Ads")
    .otherwise("Email / Awareness")
)

# ----------------------------
# 8. Final Select (clean business table)
# ----------------------------

gold = gold.select(
    "account_id",
    "company_name",
    "industry",
    "abm_score",
    "account_tier",
    "account_rank",
    "decile",
    "engagement_score",
    "conversion_rate",
    "normalized_revenue",
    "recommended_action",
    "channel_focus"
)

# ----------------------------
# Write Gold Layer
# ----------------------------

gold.write.format("delta").mode("overwrite").save(GOLD_DELTA + "abm")

print("✅ Gold layer created with advanced ABM scoring & segmentation")
