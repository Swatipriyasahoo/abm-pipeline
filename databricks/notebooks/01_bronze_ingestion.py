from pyspark.sql import SparkSession
from utils.config import *

spark = SparkSession.builder.appName("ABM Bronze Layer").getOrCreate()

# Read raw data from ADLS (written by ADF)
demandbase_df = spark.read.option("header", True).csv(DEMANDBASE_PATH)
salesforce_df = spark.read.option("header", True).csv(SALESFORCE_PATH)
linkedin_df = spark.read.option("header", True).csv(LINKEDIN_PATH)
ga4_df = spark.read.option("header", True).csv(GA4_PATH)

# Write to Delta Bronze layer
demandbase_df.write.format("delta").mode("overwrite").save(BRONZE_DELTA + "demandbase")
salesforce_df.write.format("delta").mode("overwrite").save(BRONZE_DELTA + "salesforce")
linkedin_df.write.format("delta").mode("overwrite").save(BRONZE_DELTA + "linkedin")
ga4_df.write.format("delta").mode("overwrite").save(BRONZE_DELTA + "ga4")

print("✅ Bronze layer created from ADLS")
