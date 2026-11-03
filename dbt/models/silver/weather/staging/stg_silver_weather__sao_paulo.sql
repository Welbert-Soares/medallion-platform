select
    time                                          as measured_at,
    temperature_2m                                as temperature_celsius,
    precipitation                                 as precipitation_mm,
    cast(-23.5505 as decimal(9,6))                as latitude,
    cast(-46.6333 as decimal(9,6))                as longitude,
    'sao_paulo'                                   as station_id
from {{ ref('bronze_weather__sao_paulo') }}
