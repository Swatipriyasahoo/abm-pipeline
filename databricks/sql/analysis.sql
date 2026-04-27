-- =========================================
-- SETUP: Create External Table (Gold Layer)
-- =========================================

CREATE TABLE IF NOT EXISTS abm_gold
USING DELTA
LOCATION 'abfss://bronze@yourstorageaccount.dfs.core.windows.net/delta/gold/abm';


-- =========================================
-- 1. Top Accounts for Sales Targeting
-- =========================================

CREATE OR REPLACE VIEW vw_top_accounts AS
SELECT 
    account_id,
    company_name,
    industry,
    abm_score,
    account_tier,
    account_rank,
    recommended_action
FROM abm_gold
WHERE account_tier = 'Tier 1 - High Priority';


SELECT * 
FROM vw_top_accounts
ORDER BY abm_score DESC
LIMIT 50;


-- =========================================
-- 2. Pipeline Value by Tier
-- =========================================

CREATE OR REPLACE VIEW vw_pipeline_summary AS
SELECT 
    account_tier,
    COUNT(*) AS total_accounts,
    SUM(normalized_revenue) AS total_pipeline_value,
    ROUND(AVG(abm_score), 4) AS avg_score
FROM abm_gold
GROUP BY account_tier;


SELECT * 
FROM vw_pipeline_summary
ORDER BY total_pipeline_value DESC;


-- =========================================
-- 3. Conversion Performance by Industry
-- =========================================

CREATE OR REPLACE VIEW vw_industry_performance AS
SELECT 
    industry,
    COUNT(*) AS total_accounts,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion_rate,
    ROUND(AVG(engagement_score), 4) AS avg_engagement
FROM abm_gold
GROUP BY industry;


SELECT * 
FROM vw_industry_performance
ORDER BY avg_conversion_rate DESC;


-- =========================================
-- 4. Marketing Channel Effectiveness
-- =========================================

CREATE OR REPLACE VIEW vw_channel_performance AS
SELECT 
    channel_focus,
    COUNT(*) AS total_accounts,
    ROUND(AVG(ctr), 4) AS avg_ctr,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion_rate
FROM abm_gold
GROUP BY channel_focus;


SELECT * 
FROM vw_channel_performance
ORDER BY avg_conversion_rate DESC;


-- =========================================
-- 5. Decile Distribution (Segmentation)
-- =========================================

CREATE OR REPLACE VIEW vw_decile_distribution AS
SELECT 
    decile,
    COUNT(*) AS total_accounts,
    ROUND(AVG(abm_score), 4) AS avg_score
FROM abm_gold
GROUP BY decile;


SELECT * 
FROM vw_decile_distribution
ORDER BY decile;


-- =========================================
-- 6. High Revenue but Low Engagement (Opportunity)
-- =========================================

CREATE OR REPLACE VIEW vw_opportunity_accounts AS
SELECT 
    account_id,
    company_name,
    normalized_revenue,
    engagement_score,
    recommended_action
FROM abm_gold
WHERE normalized_revenue > 0.7
  AND engagement_score < 0.3;


SELECT * 
FROM vw_opportunity_accounts
ORDER BY normalized_revenue DESC;


-- =========================================
-- 7. KPI Summary (Executive View)
-- =========================================

SELECT 
    COUNT(*) AS total_accounts,
    ROUND(AVG(abm_score), 4) AS avg_abm_score,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion_rate,
    ROUND(SUM(normalized_revenue), 4) AS total_pipeline_value
FROM abm_gold;
