{{
    config(
        materialized='table',
        unique_key='codigo_ibge',
        merge_exclude_columns=['etl_created_date'],
    )
}}

select * from {{ ref('stg_silver_ibge__municipios') }}
