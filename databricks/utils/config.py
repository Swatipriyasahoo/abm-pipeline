# =========================================
# ENVIRONMENT CONFIG
# =========================================

ENV = "dev"   # dev / qa / prod

# =========================================
# STORAGE CONFIG
# =========================================

STORAGE_ACCOUNT = "yourstorageaccount"
CONTAINER = "bronze"

BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

# =========================================
# RAW INPUT PATHS (ADF OUTPUT)
# =========================================

RAW_PATH = BASE_PATH

DEMANDBASE_RAW = RAW_PATH + "demandbase/demandbase.csv"
SALESFORCE_RAW = RAW_PATH + "salesforce/salesforce.csv"
LINKEDIN_RAW = RAW_PATH + "linkedin/linkedin.csv"
GA4_RAW = RAW_PATH + "ga4/ga4.csv"

# =========================================
# DELTA LAYER PATHS
# =========================================

DELTA_BASE = BASE_PATH + "delta/"

BRONZE_DELTA = DELTA_BASE + "bronze/"
SILVER_DELTA = DELTA_BASE + "silver/"
GOLD_DELTA = DELTA_BASE + "gold/"

# Table-specific paths
BRONZE_DEMANDBASE = BRONZE_DELTA + "demandbase"
BRONZE_SALESFORCE = BRONZE_DELTA + "salesforce"
BRONZE_LINKEDIN = BRONZE_DELTA + "linkedin"
BRONZE_GA4 = BRONZE_DELTA + "ga4"

SILVER_ABM = SILVER_DELTA + "abm"
GOLD_ABM = GOLD_DELTA + "abm"

# =========================================
# CHECKPOINT / STATE MANAGEMENT
# =========================================

CHECKPOINT_BASE = BASE_PATH + "checkpoints/"

BRONZE_CHECKPOINT = CHECKPOINT_BASE + "bronze/"
SILVER_CHECKPOINT = CHECKPOINT_BASE + "silver/"
GOLD_CHECKPOINT = CHECKPOINT_BASE + "gold/"

# =========================================
# PERFORMANCE CONFIG
# =========================================

PARTITION_COLUMN = "ingestion_date"
ZORDER_COLUMN = "account_id"

# =========================================
# PIPELINE CONFIG
# =========================================

PRIMARY_KEY = "account_id"

# ABM Scoring Weights (centralized)
ABM_WEIGHTS = {
    "intent": 0.25,
    "engagement": 0.30,
    "conversion": 0.20,
    "revenue": 0.25
}

# =========================================
# DATA QUALITY RULES
# =========================================

MIN_REVENUE = 0
MAX_INTENT_SCORE = 100

# =========================================
# VACUUM SETTINGS
# =========================================

VACUUM_RETENTION_HOURS = 168  # 7 days

# =========================================
# LOGGING CONFIG
# =========================================

LOG_LEVEL = "INFO"

# =========================================
# UTILITY FLAGS
# =========================================

ENABLE_OPTIMIZE = True
ENABLE_VACUUM = True
ENABLE_MERGE = True
