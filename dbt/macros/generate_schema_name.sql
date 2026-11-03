{#- The schema is the Source System, set per folder in dbt_project.yml, used as-is.
    dbt's default would prefix it with the profile's schema (weather → ibge_weather). -#}
{%- macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro -%}
