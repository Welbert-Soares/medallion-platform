{{
  config(
    unique_key = ['station_id', 'measured_at'],
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'weather_raw') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        CAST(time AS TIMESTAMP)               AS measured_at,
        CAST(temperature_2m  AS DECIMAL(5,2)) AS temperature_celsius,
        CAST(precipitation   AS DECIMAL(8,2)) AS precipitation_mm,
        CAST(-23.5505        AS DECIMAL(9,6)) AS latitude,
        CAST(-46.6333        AS DECIMAL(9,6)) AS longitude,
        'sao_paulo'                           AS station_id,
        _ingested_at,
        _ingestion_name
    FROM source
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY station_id, measured_at
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM renamed
)

SELECT * EXCEPT (_rn) FROM deduped WHERE _rn = 1
