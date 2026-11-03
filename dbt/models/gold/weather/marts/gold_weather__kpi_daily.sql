{{
    config(materialized='table')
}}

-- Types are the contract in gold_weather__kpi_daily.yml: cast explicitly so they never
-- depend on what avg/sum infer.
select
    reference_date,
    cast(avg_temperature as decimal(5,2))        as avg_temperature,
    cast(max_temperature as decimal(5,2))        as max_temperature,
    cast(min_temperature as decimal(5,2))        as min_temperature,
    cast(total_precipitation_mm as decimal(8,2)) as total_precipitation_mm,
    current_timestamp()                          as updated_at
from {{ ref('stg_gold_weather__kpi_daily') }}
order by reference_date desc
