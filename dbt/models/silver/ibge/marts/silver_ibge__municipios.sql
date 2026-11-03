{%- set unique_key = 'codigo_ibge' -%}

{{
    config(
        materialized='table',
        unique_key=unique_key,
        merge_exclude_columns=['etl_created_date'],
    )
}}

{{ incremental_join_where(ref('stg_silver_ibge__municipios'), unique_key) }}
