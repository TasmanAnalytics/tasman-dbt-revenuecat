with

subscription_entitlements as (
    select * from {{ ref('revenuecat_subscription_entitlements') }}
),

date_spine as (
    select * from {{ ref('revenuecat_date_spine') }}
),

final as (

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
        on cast(subscription_entitlements.valid_from as date) <= date_spine.date_day and
            (cast(subscription_entitlements.valid_to as date) >= date_spine.date_day or subscription_entitlements.valid_to is null)

    where
        subscription_status <> 'expired'

    group by
        date_day,
        country_code,
        platform,
        product_identifier

)

select * from final