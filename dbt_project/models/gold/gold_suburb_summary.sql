-- =============================================================================
-- CRE Platform — Gold Layer: Suburb Investment Summary
-- Source:  silver.stg_rentals
-- Target:  gold.gold_suburb_summary
--
-- PURPOSE: Investment-grade rental market summary by suburb.
-- TARGET USER: Real Estate Investment Analysts
--
-- KEY METRICS:
--   - Average, median, min, max weekly rent
--   - Total listings count
--   - Property type diversity
--   - Affordability score
--
-- This is the PRIMARY table an analyst would query first when
-- evaluating which suburbs offer the best rental yield potential.
-- =============================================================================

WITH silver AS (
    SELECT * FROM {{ ref('stg_rentals') }}
    WHERE has_valid_price = TRUE
      AND has_valid_locality = TRUE
),

suburb_metrics AS (
    SELECT
        locality,
        locality AS suburb,  -- Bronze has no separate suburb column, using locality
        state,

        -- -----------------------------------------------------------------------
        -- RENTAL PRICE METRICS
        -- Core investment signals — what does rent look like in this suburb?
        -- -----------------------------------------------------------------------
        COUNT(*)                                            AS total_listings,
        ROUND(AVG(weekly_rent), 2)                         AS avg_weekly_rent,
        ROUND(PERCENTILE_CONT(0.5)
              WITHIN GROUP (ORDER BY weekly_rent)::NUMERIC, 2)
                                                           AS median_weekly_rent,
        MIN(weekly_rent)                                    AS min_weekly_rent,
        MAX(weekly_rent)                                    AS max_weekly_rent,
        ROUND(STDDEV(weekly_rent)::NUMERIC, 2)             AS stddev_weekly_rent,

        -- Annual rent estimate (weekly × 52)
        ROUND(AVG(weekly_rent) * 52, 2)                    AS avg_annual_rent,

        -- -----------------------------------------------------------------------
        -- PROPERTY MIX
        -- -----------------------------------------------------------------------
        COUNT(DISTINCT property_type)                       AS property_type_count,
        MODE() WITHIN GROUP (ORDER BY property_type)        AS dominant_property_type,

        -- Bedroom distribution
        ROUND(AVG(bedrooms), 1)                            AS avg_bedrooms,
        ROUND(AVG(bathrooms), 1)                           AS avg_bathrooms,

        -- -----------------------------------------------------------------------
        -- PRICE TIER DISTRIBUTION
        -- Percentage of listings in each tier — shows market positioning
        -- -----------------------------------------------------------------------
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Budget'    THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                AS pct_budget,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Mid-Range' THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                AS pct_mid_range,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Premium'   THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                AS pct_premium,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Luxury'    THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                AS pct_luxury,

        -- -----------------------------------------------------------------------
        -- INVESTMENT SIGNAL
        -- Market competitiveness score based on listing volume and price spread
        -- Higher score = more active rental market = better liquidity for investors
        -- -----------------------------------------------------------------------
        CASE
            WHEN COUNT(*) >= 50 AND AVG(weekly_rent) >= 600  THEN 'High Demand - Premium'
            WHEN COUNT(*) >= 50 AND AVG(weekly_rent) < 600   THEN 'High Demand - Affordable'
            WHEN COUNT(*) >= 20 AND AVG(weekly_rent) >= 600  THEN 'Moderate Demand - Premium'
            WHEN COUNT(*) >= 20 AND AVG(weekly_rent) < 600   THEN 'Moderate Demand - Affordable'
            WHEN COUNT(*) >= 5                                THEN 'Low Demand'
            ELSE 'Sparse Market'
        END                                                 AS market_signal,

        -- Audit
        MAX(_ingested_at)                                   AS last_ingested_at

    FROM silver
    GROUP BY locality, state
)

SELECT
    *,
    -- Rank suburbs by avg rent within each state — useful for state-level comparison
    RANK() OVER (
        PARTITION BY state
        ORDER BY avg_weekly_rent DESC
    ) AS rent_rank_in_state

FROM suburb_metrics
ORDER BY avg_weekly_rent DESC
