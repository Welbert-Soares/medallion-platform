select
    date(measured_at)                        as reference_date,
    round(avg(temperature_celsius), 2)       as avg_temperature,
    round(max(temperature_celsius), 2)       as max_temperature,
    round(min(temperature_celsius), 2)       as min_temperature,
    round(sum(precipitation_mm), 2)          as total_precipitation_mm
from {{ ref('silver_weather__sao_paulo') }}
group by date(measured_at)
