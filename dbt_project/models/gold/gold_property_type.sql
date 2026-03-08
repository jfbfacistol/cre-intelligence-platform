-- =============================================================================
-- CRE Platform — Gold Layer: Property Type Analysis
-- Source:  silver.stg_rentals
-- Target:  gold.gold_property_type
--
-- PURPOSE: Rental market breakdown by property type.
-- INSIGHT: Helps analysts understand which property types command
--          premium rents and where supply is concentrated.
-- =============================================================================

WITH silver AS (
    SELECT * FROM {{ ref('stg_rentals') }}
    WHERE has_valid_price = TRUE
),

property_metrics AS (
    SELECT
        property_type,
        state,

        COUNT(*)                                            AS total_listings,
        ROUND(AVG(weekly_rent), 2)                         AS avg_weekly_rent,
        ROUND(PERCENTILE_CONT(0.5)
              WITHIN GROUP (ORDER BY weekly_rent)::NUMERIC, 2)
                                                           AS median_weekly_rent,
        MIN(weekly_rent)                                    AS min_weekly_rent,
        MAX(weekly_rent)                                    AS max_weekly_rent,
        ROUND(AVG(weekly_rent) * 52, 2)                    AS avg_annual_rent,

        -- Average property size metrics
        ROUND(AVG(bedrooms), 1)                            AS avg_bedrooms,
        ROUND(AVG(bathrooms), 1)                           AS avg_bathrooms,
        ROUND(AVG(parking_spaces), 1)                      AS avg_parking,

        -- Rent per bedroom — key yield indicator
        ROUND(
            AVG(CASE WHEN bedrooms > 0
                THEN weekly_rent / bedrooms
                ELSE NULL END
            ), 2
        )                                                  AS avg_rent_per_bedroom,

        -- Market share within state
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
            PARTITION BY state
        ), 1)                                              AS pct_of_state_market,

        MAX(_ingested_at)                                  AS last_ingested_at

    FROM silver
    WHERE property_type IS NOT NULL
    GROUP BY property_type, state
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY state
        ORDER BY avg_weekly_rent DESC
    ) AS rent_rank_in_state

FROM property_metrics
ORDER BY state, avg_weekly_rent DESC
