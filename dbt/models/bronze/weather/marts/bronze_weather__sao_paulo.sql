{%- set unique_key = 'time' -%}

{{
    config(
        materialized='incremental',
        unique_key=unique_key,
        merge_exclude_columns=['etl_created_date'],
    )
}}

{{ incremental_join_where(ref('stg_bronze_weather__sao_paulo'), unique_key) }}
