select
    codigo_ibge,
    nome,
    uf,
    capital                    as is_capital,
    latitude,
    longitude
from {{ ref('bronze_ibge__municipios') }}
