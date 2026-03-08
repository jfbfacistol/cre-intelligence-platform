-- =============================================================================
-- CRE Platform — Gold Layer: State Summary
-- Source:  silver.stg_rentals
-- Target:  gold.gold_state_summary
--
-- PURPOSE: High-level national rental market overview by Australian state.
-- INSIGHT: Top-level executive dashboard view — which states are most
--          expensive, have most supply, and best investment signals.
-- =============================================================================

WITH silver AS (
    SELECT * FROM {{ ref('stg_rentals') }}
    WHERE has_valid_price  = TRUE
      AND has_valid_locality = TRUE
),

state_metrics AS (
    SELECT
        state,

        -- Volume
        COUNT(*)                                            AS total_listings,
        COUNT(DISTINCT locality)                            AS unique_localities,
        COUNT(DISTINCT property_type)                       AS property_types_available,

        -- Rent metrics
        ROUND(AVG(weekly_rent), 2)                         AS avg_weekly_rent,
        ROUND(PERCENTILE_CONT(0.5)
              WITHIN GROUP (ORDER BY weekly_rent)::NUMERIC, 2)
                                                           AS median_weekly_rent,
        MIN(weekly_rent)                                    AS min_weekly_rent,
        MAX(weekly_rent)                                    AS max_weekly_rent,
        ROUND(AVG(weekly_rent) * 52, 2)                    AS avg_annual_rent,

        -- Affordability — % of listings under $500/week
        ROUND(100.0 * SUM(CASE WHEN weekly_rent < 500 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                               AS pct_affordable_under_500,

        -- Luxury market — % of listings over $1200/week
        ROUND(100.0 * SUM(CASE WHEN weekly_rent > 1200 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                               AS pct_luxury_over_1200,

        -- Property mix
        ROUND(AVG(bedrooms), 1)                            AS avg_bedrooms,
        ROUND(AVG(bathrooms), 1)                           AS avg_bathrooms,

        -- Most common property type in state
        MODE() WITHIN GROUP (ORDER BY property_type)        AS dominant_property_type,

        -- Most expensive suburb in state
        MAX(locality)                                       AS sample_locality,

        MAX(_ingested_at)                                  AS last_ingested_at

    FROM silver
    WHERE state IS NOT NULL
      AND TRIM(state) != ''
    GROUP BY state
)

SELECT
    *,
    -- National rank by average rent
    RANK() OVER (ORDER BY avg_weekly_rent DESC)            AS national_rent_rank,

    -- % of total national listings
    ROUND(100.0 * total_listings / SUM(total_listings) OVER (), 1)
                                                           AS pct_of_national_market

FROM state_metrics
ORDER BY avg_weekly_rent DESC
