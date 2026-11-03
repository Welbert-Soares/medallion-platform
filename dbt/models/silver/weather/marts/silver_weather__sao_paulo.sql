{%- set unique_key = 'measured_at' -%}

{{
    config(
        materialized='incremental',
        unique_key=unique_key,
        merge_exclude_columns=['etl_created_date'],
    )
}}

{{ incremental_join_where(ref('stg_silver_weather__sao_paulo'), unique_key) }}
