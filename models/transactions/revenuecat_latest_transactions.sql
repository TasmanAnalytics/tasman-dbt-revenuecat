{{
    config(
        materialized='table',
    )
}}

with

revenuecat_transactions as (
    select * from {{ ref('revenuecat_all_transactions') }} where valid_to is null
),

classified_transactions as (
    select
        revenuecat_transactions.*,
        refunded_at is not null as is_refunded,
        grace_period_end_time is not null as is_grace_period
    
    from
        revenuecat_transactions
)

select * from classified_transactions
