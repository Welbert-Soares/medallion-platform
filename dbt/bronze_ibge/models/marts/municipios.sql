{%- set unique_key = 'codigo_ibge' -%}

{{
    config(
        unique_key=unique_key,
        merge_exclude_columns=['etl_created_date'],
    )
}}

{{ utils.incremental_join_where(ref('stg_municipios'), unique_key) }}
