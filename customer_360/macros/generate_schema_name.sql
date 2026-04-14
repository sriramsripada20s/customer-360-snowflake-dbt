-- generate_schema_name.sql
-- Purpose : Override dbt default schema naming
--
-- Default dbt behavior:
--   schema = target.schema + '_' + custom_schema
--   e.g. RAW_STAGE, RAW_MART, RAW_FEATURES
--
-- Our behavior:
--   dev  → DEV_STAGE, DEV_MART, DEV_FEATURES
--   prod → STAGE, MART, FEATURES (exact schema names)

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {# Production: use exact schema name #}
        {{ custom_schema_name | upper | trim }}

    {%- else -%}
        {# Development: prefix with target schema #}
        {{ default_schema }}_{{ custom_schema_name | upper | trim }}

    {%- endif -%}

{%- endmacro %}