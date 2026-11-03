{{
    config(materialized='table')
}}

select
    reference_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    total_precipitation_mm,
    current_timestamp() as updated_at
from {{ ref('stg_gold_weather__kpi_daily') }}
order by reference_date desc
