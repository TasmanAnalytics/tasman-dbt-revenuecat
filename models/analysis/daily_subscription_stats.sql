with

subscription_entitlements as (

    select * from {{ ref('revenuecat_subscription_entitlements') }}

),

subscription_transactions as (

    select * from {{ ref('revenuecat_subscription_transactions') }}

),

subscription_products as (
    
    select * from {{ ref('revenuecat_subscription_products') }}
    
),

date_spine as (

    select * from {{ ref('date_spine') }}

),

subscribers as (

    select
        date_spine.date_day,
        country_code,
        platform,
        product_identifier,
        count(distinct case when is_trial_period then rc_original_app_user_id end) as trial_subscriber_count,
        count(distinct case when not (is_trial_period) then rc_original_app_user_id end) as paid_subscriber_count

    from
        date_spine

    left join subscription_entitlements
        on subscription_entitlements.valid_to::date > date_spine.date_day
        and subscription_entitlements.valid_from::date <= date_spine.date_day

    where
        subscription_status <> 'expired'

    group by
        date_day,
        country_code,
        platform,
        product_identifier

),

subscriptions as (

    select
        date_spine.date_day,
        country_code,
        platform,
        product_identifier,
        count(distinct case when is_trial_period then store_transaction_id end) as trial_subscription_count,
        count(distinct case when not(is_trial_period) then store_transaction_id end) as paid_subscription_count,
        sum(case when is_new_revenue then price_in_usd end) as new_revenue_in_usd,
        sum(case when not(is_new_revenue) then price_in_usd end) as renewal_revenue_in_usd,
        sum(price_in_usd) as total_revenue_in_usd,
        sum(commission_in_usd) as commission_in_usd,
        sum(estimated_tax_in_usd) as estimated_tax_in_usd,
        sum(proceeds_in_usd) as proceeds_in_usd

    from
        date_spine
    
    left join 
        subscription_transactions
        on subscription_transactions.effective_end_time::date > date_spine.date_day
        and subscription_transactions.start_time::date <= date_spine.date_day
    
    group by
        date_day,
        country_code,
        platform,
        product_identifier

),

final as (

    select 
        coalesce(subscribers.date_day, subscriptions.date_day) as date_day,
        coalesce(subscribers.country_code, subscriptions.country_code) as country_code,
        coalesce(subscribers.platform, subscriptions.platform) as platform,
        coalesce(subscribers.product_identifier, subscriptions.product_identifier) as product_identifier,
        subscribers.trial_subscriber_count,
        subscribers.paid_subscriber_count,
        subscriptions.trial_subscription_count,
        subscriptions.paid_subscription_count,
        subscriptions.new_revenue_in_usd,
        subscriptions.renewal_revenue_in_usd,
        subscriptions.total_revenue_in_usd,
        subscriptions.commission_in_usd,
        subscriptions.estimated_tax_in_usd,
        subscriptions.proceeds_in_usd
        
    from 
        subscribers
    
    full outer join
        subscriptions
        on subscribers.date_day = subscriptions.date_day
        and subscribers.country_code = subscriptions.country_code
        and subscribers.platform = subscriptions.platform
        and subscribers.product_identifier = subscriptions.product_identifier

)

select * from final
