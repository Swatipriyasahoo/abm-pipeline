# ADLS base path
BASE_PATH = "abfss://bronze@yourstorageaccount.dfs.core.windows.net/"

# Raw files written by ADF
DEMANDBASE_PATH = BASE_PATH + "demandbase/demandbase.csv"
SALESFORCE_PATH = BASE_PATH + "salesforce/salesforce.csv"
LINKEDIN_PATH = BASE_PATH + "linkedin/linkedin.csv"
GA4_PATH = BASE_PATH + "ga4/ga4.csv"

# Delta output paths
BRONZE_DELTA = BASE_PATH + "delta/bronze/"
SILVER_DELTA = BASE_PATH + "delta/silver/"
GOLD_DELTA = BASE_PATH + "delta/gold/"
