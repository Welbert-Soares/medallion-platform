select
    cast(trim(codigo_ibge) as bigint)  as codigo_ibge,
    trim(nome)                         as nome,
    upper(trim(uf))                    as uf,
    cast(capital as boolean)           as capital,
    cast(latitude  as decimal(9,6))    as latitude,
    cast(longitude as decimal(9,6))    as longitude,
    _ingested_at,
    _ingestion_name
from {{ source('ibge', 'municipios') }}
where codigo_ibge is not null
-- Raw is append-only (ADR 0002): keep the latest load of each municipality.
qualify row_number() over (partition by codigo_ibge order by _ingested_at desc) = 1
