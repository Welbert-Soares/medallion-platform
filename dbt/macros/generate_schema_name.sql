-- Overrides dbt default so silver/gold models land in cli_catalog.silver / cli_catalog.gold
-- instead of cli_catalog.silver_silver (the default prefix behavior).
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
