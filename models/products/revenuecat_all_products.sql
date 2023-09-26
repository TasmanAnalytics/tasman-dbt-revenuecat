{{
    config(
        materialized='table',
    )
}}

with

revenuecat_transactions as (
    select * from {{ ref('revenuecat_all_transactions') }}
),

products_over_time as (
    select
        product_identifier,
        product_display_name,
        product_duration,
        min(start_time) as first_subscribed_at,
        max(start_time) as latest_subscribed_at

    from
        revenuecat_transactions
    
    group by
        product_identifier,
        product_display_name,
        product_duration
)

select * from products_over_time
