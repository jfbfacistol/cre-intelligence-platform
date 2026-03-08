-- =============================================================================
-- CRE Platform — Silver Layer: stg_rentals
-- Source:  bronze.australian_rentals
-- Target:  silver.stg_rentals
--
-- PURPOSE: Clean, type-cast, and validate raw Bronze data.
-- COLUMNS: Mapped to actual bronze column names (_raw suffix versions)
-- IDEMPOTENT: Materialized as TABLE with full refresh.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'australian_rentals') }}
),

cleaned AS (
    SELECT
        -- -----------------------------------------------------------------------
        -- AUDIT COLUMNS — preserved from Bronze for lineage
        -- -----------------------------------------------------------------------
        _pipeline_run_id,
        _ingested_at,
        _source_file,
        _dq_warnings,

        -- -----------------------------------------------------------------------
        -- PRICE PARSING
        -- Raw: 'price_display_raw' contains numeric strings e.g. '1200', '420'
        -- Silver: Cast to NUMERIC for aggregation
        -- -----------------------------------------------------------------------
        CASE
            WHEN TRIM(price_display_raw) ~ '^\d+(\.\d+)?$'
                THEN TRIM(price_display_raw)::NUMERIC
            ELSE NULL
        END AS weekly_rent,

        -- Price tier classification for investment analysis
        CASE
            WHEN TRIM(price_display_raw) ~ '^\d+(\.\d+)?$' THEN
                CASE
                    WHEN TRIM(price_display_raw)::NUMERIC < 400  THEN 'Budget'
                    WHEN TRIM(price_display_raw)::NUMERIC < 700  THEN 'Mid-Range'
                    WHEN TRIM(price_display_raw)::NUMERIC < 1200 THEN 'Premium'
                    ELSE 'Luxury'
                END
            ELSE 'Unknown'
        END AS price_tier,

        -- -----------------------------------------------------------------------
        -- PROPERTY ATTRIBUTES
        -- -----------------------------------------------------------------------
        TRIM(LOWER(property_type))                          AS property_type,
        INITCAP(TRIM(locality))                             AS locality,
        UPPER(TRIM(state))                                  AS state,
        TRIM(postcode)                                      AS postcode,
        TRIM(agency_name)                                   AS agency_name,
        TRIM(amenities_raw)                                 AS amenities,
        TRIM(description_raw)                               AS description,
        TRIM(source_url)                                    AS source_url,

        -- -----------------------------------------------------------------------
        -- NUMERIC CASTING
        -- _raw columns may contain strings like '3+' — extract first digit safely
        -- -----------------------------------------------------------------------
        CASE
            WHEN bedrooms_raw ~ '^\d+'
                THEN REGEXP_REPLACE(bedrooms_raw, '[^0-9]', '', 'g')::INTEGER
            ELSE NULL
        END AS bedrooms,

        CASE
            WHEN bathrooms_raw ~ '^\d+'
                THEN REGEXP_REPLACE(bathrooms_raw, '[^0-9]', '', 'g')::INTEGER
            ELSE NULL
        END AS bathrooms,

        CASE
            WHEN parking_raw ~ '^\d+'
                THEN REGEXP_REPLACE(parking_raw, '[^0-9]', '', 'g')::INTEGER
            ELSE NULL
        END AS parking_spaces,

        -- -----------------------------------------------------------------------
        -- DATE PARSING
        -- listed_date_raw is a raw string — attempt safe cast
        -- -----------------------------------------------------------------------
        CASE
            WHEN listed_date_raw IS NOT NULL
                AND TRIM(listed_date_raw) != ''
                THEN
                    CASE
                        WHEN listed_date_raw ~ '^\d{4}-\d{2}-\d{2}'
                            THEN listed_date_raw::DATE
                        ELSE NULL
                    END
            ELSE NULL
        END AS listed_date,

        -- -----------------------------------------------------------------------
        -- DATA QUALITY FLAGS
        -- -----------------------------------------------------------------------
        CASE
            WHEN TRIM(price_display_raw) ~ '^\d+(\.\d+)?$'
                AND TRIM(price_display_raw)::NUMERIC > 0
            THEN TRUE ELSE FALSE
        END AS has_valid_price,

        CASE
            WHEN locality IS NOT NULL
                AND TRIM(locality) != ''
            THEN TRUE ELSE FALSE
        END AS has_valid_locality,

        CASE
            WHEN bedrooms_raw ~ '^\d+' THEN TRUE ELSE FALSE
        END AS has_valid_bedrooms

    FROM source
    WHERE
        locality IS NOT NULL
        AND TRIM(locality) != ''
)

SELECT * FROM cleaned
