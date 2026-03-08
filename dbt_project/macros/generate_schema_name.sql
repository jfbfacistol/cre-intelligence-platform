-- =============================================================================
-- CRE Platform — Custom Schema Name Macro
--
-- PROBLEM: By default dbt concatenates the target schema + custom schema.
-- So with target=gold and model schema=silver, dbt creates "gold_silver".
--
-- FIX: This macro tells dbt to use the custom schema name directly,
-- ignoring the target schema prefix entirely.
--
-- RESULT:
--   silver models → silver.stg_rentals        (not gold_silver.stg_rentals)
--   gold models   → gold.gold_suburb_summary  (not gold_gold.gold_suburb_summary)
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
