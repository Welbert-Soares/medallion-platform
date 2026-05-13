{{
  config(
    unique_key = ['codigo_ibge'],
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'ibge_municipios_raw') }}
),

renamed AS (
    SELECT
        CAST(TRIM(codigo_ibge) AS BIGINT) AS codigo_ibge,
        TRIM(nome)                        AS nome,
        UPPER(TRIM(uf))                   AS uf,
        CAST(capital AS BOOLEAN)          AS is_capital,
        CAST(latitude  AS DECIMAL(9,6))   AS latitude,
        CAST(longitude AS DECIMAL(9,6))   AS longitude,
        _ingested_at,
        _ingestion_name
    FROM source
    WHERE codigo_ibge IS NOT NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY codigo_ibge
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM renamed
)

SELECT * EXCEPT (_rn) FROM deduped WHERE _rn = 1
