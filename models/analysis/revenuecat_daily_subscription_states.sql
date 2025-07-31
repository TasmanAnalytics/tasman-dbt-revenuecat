with

subscription_entitlements as (
    select * from {{ ref('revenuecat_subscription_entitlements') }}
),

subscription_activities as (
    select * from {{ ref('revenuecat_subscription_activities') }}
),

subscription_transactions as (
    select * from {{ ref('revenuecat_subscription_transactions') }} where valid_to is null
),

subscription_products as (
    select * from {{ ref('revenuecat_subscription_products') }}
),

date_spine as (
    select * from {{ ref('revenuecat_date_spine') }}
),

flattened_dimensions as (
    {{ tasman_dbt_revenuecat.flatten_arrays(
        relation='subscription_products',
        array_columns=['country_codes', 'platforms'],
        value_columns=['country_code', 'platform']
    ) }}
),

date_spine_dimensions as (

    select
        date_spine.date_day,
        flattened_dimensions.product_identifier,
        flattened_dimensions.country_code,
        flattened_dimensions.platform

    from
        date_spine,
        flattened_dimensions
    
    where 
        date_spine.date_day >= cast(flattened_dimensions.first_subscribed_at as date)
        and date_spine.date_day <= cast(flattened_dimensions.latest_effective_end_time as date)

),

new_subscribers as (

    select
        cast(activity_timestamp as date) as subscription_started_date,
        country_code,
        platform,
        product_identifier,
        case
            when is_trial_period = true then 'new_trial'
            when renewal_number = 1 and is_trial_period = false then 'new_paid'
            when renewal_number = 2 and is_trial_conversion = true then 'converted'
        end as subscription_type

    from
        subscription_activities

    where
        activity = 'subscription_started'

),

subscription_states as (

    select
        valid_from,
        country_code,
        platform,
        product_identifier,
        subscription_status

    from
        subscription_entitlements

    where
        subscription_status in ('crossgrade', 'downgrade', 'cancelled', 'expired', 'renewed', 'reactivated')

),

daily_new_subscriptions as (

    select
        date_spine_dimensions.date_day,
        date_spine_dimensions.country_code,
        date_spine_dimensions.platform,
        date_spine_dimensions.product_identifier,
        sum(case when new_subscribers.subscription_type = 'new_trial' then 1 else 0 end) as count_of_new_trial,
        sum(case when new_subscribers.subscription_type = 'new_paid' then 1 else 0 end) as count_of_new_paid,
        sum(case when new_subscribers.subscription_type = 'converted' then 1 else 0 end) as count_of_converted

    from
        date_spine_dimensions

    left join
        new_subscribers
        on date_spine_dimensions.date_day = new_subscribers.subscription_started_date
        and date_spine_dimensions.country_code = new_subscribers.country_code
        and date_spine_dimensions.platform = new_subscribers.platform
        and date_spine_dimensions.product_identifier = new_subscribers.product_identifier
        and new_subscribers.subscription_type is not null

    group by
        date_spine_dimensions.date_day,
        date_spine_dimensions.country_code,
        date_spine_dimensions.platform,
        date_spine_dimensions.product_identifier

),

daily_subscriptions_state as (

    select
        date_spine_dimensions.date_day,
        date_spine_dimensions.country_code,
        date_spine_dimensions.platform,
        date_spine_dimensions.product_identifier,
        sum(case when subscription_states.subscription_status = 'crossgrade' then 1 else 0 end) as count_of_crossgrade,
        sum(case when subscription_states.subscription_status = 'downgrade' then 1 else 0 end) as count_of_downgrade,
        sum(case when subscription_states.subscription_status = 'cancelled' then 1 else 0 end) as count_of_cancelled,
        sum(case when subscription_states.subscription_status = 'expired' then 1 else 0 end) as count_of_expired,
        sum(case when subscription_states.subscription_status = 'renewed' then 1 else 0 end) as count_of_renewed,
        sum(case when subscription_states.subscription_status = 'reactivated' then 1 else 0 end) as count_of_reactivated

    from
        date_spine_dimensions

    left join
        subscription_states
        on date_spine_dimensions.date_day = cast(subscription_states.valid_from as date)
        and date_spine_dimensions.country_code = subscription_states.country_code
        and date_spine_dimensions.platform = subscription_states.platform

    group by
        date_spine_dimensions.date_day,
        date_spine_dimensions.country_code,
        date_spine_dimensions.platform,
        date_spine_dimensions.product_identifier

),

final as (

    select
        {{ tasman_dbt_revenuecat.generate_surrogate_key([
            'daily_new_subscriptions.date_day',
            'daily_new_subscriptions.country_code',
            'daily_new_subscriptions.platform',
            'daily_new_subscriptions.product_identifier'
        ])}} as row_id,
        daily_new_subscriptions.date_day,
        daily_new_subscriptions.country_code,
        daily_new_subscriptions.platform,
        daily_new_subscriptions.product_identifier,
        daily_new_subscriptions.count_of_new_trial,
        daily_new_subscriptions.count_of_new_paid,
        daily_new_subscriptions.count_of_converted,
        daily_subscriptions_state.count_of_crossgrade,
        daily_subscriptions_state.count_of_downgrade,
        daily_subscriptions_state.count_of_cancelled,
        daily_subscriptions_state.count_of_expired,
        daily_subscriptions_state.count_of_renewed,
        daily_subscriptions_state.count_of_reactivated

    from
        daily_new_subscriptions

    left join
        daily_subscriptions_state
        on daily_new_subscriptions.date_day = daily_subscriptions_state.date_day
        and daily_new_subscriptions.country_code = daily_subscriptions_state.country_code
        and daily_new_subscriptions.platform = daily_subscriptions_state.platform
        and daily_new_subscriptions.product_identifier = daily_subscriptions_state.product_identifier

)

select * from final
