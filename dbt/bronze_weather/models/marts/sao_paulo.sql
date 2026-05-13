{%- set unique_key = 'time' -%}

{{
    config(
        unique_key=unique_key,
        merge_exclude_columns=['etl_created_date'],
    )
}}

{{ utils.incremental_join_where(ref('stg_sao_paulo'), unique_key) }}
