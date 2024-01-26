with

subscription_transactions as (

    select * from {{ ref('revenuecat_subscription_transactions') }}

),

date_spine as (

    select * from {{ ref('date_spine') }}

),

final as (

    select
        start_time::date as date_day,
        country_code,
        platform,
        product_identifier,
        sum(case when is_trial_period then price_in_usd end) as new_revenue_in_usd,
        sum(case when not (is_trial_period) then price_in_usd end) as renewal_revenue_in_usd,
        sum(price_in_usd) as total_revenue_in_usd,
        sum(commission_in_usd) as commission_in_usd,
        sum(estimated_tax_in_usd) as estimated_tax_in_usd,
        sum(proceeds_in_usd) as proceeds_in_usd
    
    from
        subscription_transactions
    
    group by
        date_day,
        country_code,
        platform,
        product_identifier

)

select * from final
