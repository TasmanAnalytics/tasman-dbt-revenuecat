with

activites as (

    select * from {{ ref('revenuecat_subscription_activities') }}

),

entitlements as (

    select
        rc_original_app_user_id,
        rc_last_seen_app_user_id_alias,
        store_transaction_id,
        country_code,
        platform,
        product_identifier,
        activity,
        renewal_number,
        is_trial_period,
        is_trial_conversion,
        activity_timestamp as valid_from,
        lag(activity_timestamp) over (partition by rc_last_seen_app_user_id_alias order by renewal_number desc, activity_timestamp desc) as valid_to

    from
        activites

),

valid_periods as (

    select
        *,
        timestampdiff(hours, valid_from, valid_to) as valid_period
    from
        entitlements

),

previous_products as (

    select
        *,
        case
            when activity = 'subscription_started'
                then lag(product_identifier) over (partition by rc_last_seen_app_user_id_alias order by valid_from)
        end as previous_product,
        product_identifier <> previous_product as is_product_changed

    from
        valid_periods

),

subscription_states as (

    select
        *,
        case
            when activity = 'subscription_started' and is_trial_period then 'active_trial'
            when activity = 'subscription_started' and not (is_trial_period) then 'active'
            when activity = 'billing_issue_detected' then 'grace_period'
            when activity = 'subscription_cancelled' then 'cancelled'
            when activity = 'subscription_refunded' then 'refunded'
            when activity in ('subscription_ended', 'grace_period_ended') then 'churned'
        end as subscription_state

    from
        previous_products

    where
        valid_period > 0
        or valid_period is null

),

previous_states as (

    select
        *,
        lag(subscription_state) over (partition by rc_last_seen_app_user_id_alias order by valid_from) as previous_subscription_state

    from
        subscription_states

),

subscription_statuses as (

    select
        *,
        case
            when subscription_state = 'active' and previous_subscription_state = 'active' and not (is_product_changed) then 'renewed'
            when subscription_state = 'active' and previous_subscription_state = 'churned' then 'reactivated'
            else subscription_state
        end as subscription_status

    from
        previous_states

    order by
        renewal_number,
        valid_from

),

final as (
    select
        {{ tasman_dbt_revenuecat.generate_surrogate_key(['store_transaction_id', 'subscription_status', 'valid_from'])}} as entitlement_row_id,
        *
    from
        subscription_statuses
)

select * from final
