select
    cast(time as timestamp)               as time,
    cast(temperature_2m  as decimal(5,2)) as temperature_2m,
    cast(precipitation   as decimal(8,2)) as precipitation,
    _ingested_at,
    _ingestion_name
from {{ source('weather', 'sao_paulo') }}
qualify row_number() over (partition by time order by _ingested_at desc) = 1
