select
    codigo_ibge,
    nome,
    uf,
    capital                    as is_capital,
    latitude,
    longitude
from {{ source('bronze_ibge', 'municipios') }}
