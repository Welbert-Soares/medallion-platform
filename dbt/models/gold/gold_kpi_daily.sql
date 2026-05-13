{{ config(materialized='table') }}

WITH weather_daily AS (
    SELECT
        DATE(measured_at)          AS reference_date,
        AVG(temperature_celsius)   AS avg_temperature,
        MAX(temperature_celsius)   AS max_temperature,
        MIN(temperature_celsius)   AS min_temperature,
        SUM(precipitation_mm)      AS total_precipitation_mm
    FROM {{ ref('silver_weather') }}
    GROUP BY DATE(measured_at)
)

SELECT
    reference_date,
    ROUND(avg_temperature, 2)       AS avg_temperature,
    ROUND(max_temperature, 2)       AS max_temperature,
    ROUND(min_temperature, 2)       AS min_temperature,
    ROUND(total_precipitation_mm, 2) AS total_precipitation_mm,
    current_timestamp()             AS updated_at
FROM weather_daily
ORDER BY reference_date DESC
