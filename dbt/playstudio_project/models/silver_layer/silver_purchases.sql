{{
  config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key = 'transaction_id',
    tags = ['silver_layer', 'purchases_data', 'silver_5mins_incremental']
  )
}}



with processed_purchases as (
    SELECT
        CAST(timestamp AS TIMESTAMP) AS timestamp,
        CAST(transaction_id AS TEXT) AS transaction_id,
        CAST(user_id AS TEXT) AS user_id,
        COALESCE(CAST(SPLIT_PART(revenue, '=', 2) AS FLOAT), 0.0) AS revenue,
        CAST(extracted_at AS TIMESTAMP) AS extracted_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(created_at AS TIMESTAMP) AS created_at,
        row_number() over (partition by transaction_id order by updated_at desc) as rn
    FROM {{ source('bronze_layer_source', 'bronze_purchases') }}
    {% if is_incremental() %}
        WHERE timestamp >= (SELECT MAX(timestamp) FROM {{ this }}) - INTERVAL '10 minutes'
    {% endif %}
)
select 
    timestamp,
    transaction_id,
    user_id,
    revenue,
    extracted_at,
    updated_at,
    created_at
from processed_purchases
where rn = 1



