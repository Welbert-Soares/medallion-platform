-- Overrides dbt default so models land in the exact schema defined in dbt_project.yml
-- instead of appending the target.schema prefix (e.g. silver_weather instead of weather_weather).
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
