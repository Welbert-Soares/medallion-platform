select
    codigo_ibge,
    nome,
    uf,
    capital                    as is_capital,
    latitude,
    longitude,
    etl_created_date,
    etl_updated_date
from {{ source('bronze_ibge', 'municipios') }}
