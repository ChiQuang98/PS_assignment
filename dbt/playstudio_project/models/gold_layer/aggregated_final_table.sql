{{
  config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key = ['app_hour', 'user_id'],
    tags = ['gold_layer', 'aggregated_final_table', 'hourly_incremental']
  )
}}

with aggregated_purchases_hourly as (
    SELECT
        DATE_TRUNC('hour', timestamp) as app_hour,
        user_id,
        SUM(revenue) as total_revenue_hourly,
        COUNT(transaction_id) as amount_purchases_hourly,
        SUM(revenue)/COUNT(transaction_id) as avg_revenue_per_purchase_hourly
    FROM {{ ref('silver_purchases') }}
    {% if is_incremental() %}
        WHERE timestamp >= (SELECT MAX(app_hour) FROM {{ this }}) - INTERVAL '2 hours'
    {% endif %}
    GROUP BY DATE_TRUNC('hour', timestamp), user_id

), 
aggregated_spins_hourly as (
    SELECT 
        DATE_TRUNC('hour', timestamp) as app_hour,
        user_id,
        country,
        sum(total_spins) as total_spins_hourly
    FROM {{ ref('silver_spins_hourly') }}
    {% if is_incremental() %}
        WHERE timestamp >= (SELECT MAX(app_hour) FROM {{ this }}) - INTERVAL '2 hours'
    {% endif %}
    GROUP BY DATE_TRUNC('hour', timestamp), user_id, country
), combined_data as (
    select 
        coalesce(ash.app_hour, aph.app_hour)::timestamp as app_hour,
        coalesce(ash.user_id, aph.user_id)::text as user_id,
        coalesce(ash.country, 'UNKNOWN')::text as country,
        CEIL(coalesce(ash.total_spins_hourly, 0))::int as total_spins_hourly,
        coalesce(aph.total_revenue_hourly, 0)::float as total_revenue_hourly,
        coalesce(aph.amount_purchases_hourly, 0)::int as amount_purchases_hourly,
        coalesce(aph.avg_revenue_per_purchase_hourly, 0)::float as avg_revenue_per_purchase_hourly
    from aggregated_spins_hourly as ash
    full outer join aggregated_purchases_hourly as aph
        on ash.app_hour = aph.app_hour
        and ash.user_id = aph.user_id
)
select 
    app_hour,
    user_id,
    country,
    total_revenue_hourly,
    amount_purchases_hourly,
    avg_revenue_per_purchase_hourly,
    total_spins_hourly,
    CAST(SUM(total_revenue_hourly) over (PARTITION BY DATE_TRUNC('day', app_hour), user_id) as float) as total_revenue_per_user_daily
from combined_data
order by app_hour asc


    



