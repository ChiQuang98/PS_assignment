{{
  config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key = ['timestamp', 'user_id', 'country'],
    tags = ['silver_layer', 'purchases_data', 'silver_hourly_incremental']
  )
}}



with processed_spins_hourly as (
    SELECT
        CAST(timestamp AS TIMESTAMP) AS timestamp,
        CAST(user_id AS TEXT) AS user_id,
        CAST(country AS TEXT) AS country,
        CAST(total_spins AS FLOAT) AS total_spins,
        CAST(extracted_at AS TIMESTAMP) AS extracted_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(created_at AS TIMESTAMP) AS created_at,
        row_number() over (partition by timestamp, user_id, country order by updated_at desc) as rn
    FROM {{ source('bronze_layer_source', 'bronze_spins_hourly') }}
    {% if is_incremental() %}
        WHERE timestamp >= (SELECT MAX(timestamp) FROM {{ this }}) - INTERVAL '2 hours'
    {% endif %}
)
select 
    timestamp,
    user_id,
    country,
    total_spins,
    extracted_at,
    updated_at,
    created_at
from processed_spins_hourly
where rn = 1



