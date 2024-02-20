with

revenuecat_transactions as (

    select * from {{ ref('revenuecat_subscription_transactions') }}

),

final as (

    select
        product_identifier,
        product_display_name,
        product_duration,
        min(start_time) as first_subscribed_at,
        max(start_time) as latest_subscribed_at,
        max(effective_end_time) as latest_effective_end_time,
        array_agg(distinct country_code) as country_codes,
        array_agg(distinct platform) as platforms

    from
        revenuecat_transactions

    group by
        product_identifier,
        product_display_name,
        product_duration

)

select * from final
