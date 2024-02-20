with

subscription_transactions as (
    select * from {{ ref('revenuecat_subscription_transactions') }} where valid_to is null
),

date_spine as (
    select distinct date_month from {{ ref('revenuecat_date_spine') }}
),

final as (

    select
        {{ tasman_dbt_revenuecat.generate_surrogate_key([
            'date_spine.date_month',
            'subscription_transactions.country_code',
            'subscription_transactions.platform',
            'subscription_transactions.product_identifier'
        ])}} as row_id,
        date_spine.date_month,
        subscription_transactions.country_code,
        subscription_transactions.platform,
        subscription_transactions.product_identifier,
        count(distinct case when subscription_transactions.is_trial_period then subscription_transactions.store_transaction_id end) as trial_subscription_count,
        count(distinct case when not (subscription_transactions.is_trial_period) then subscription_transactions.store_transaction_id end) as paid_subscription_count

    from
        date_spine

    left join
        subscription_transactions
        on last_day(date_spine.date_month, month) >= subscription_transactions.start_time::date
        and last_day(date_spine.date_month, month) < subscription_transactions.effective_end_time::date

    group by
        date_spine.date_month,
        subscription_transactions.country_code,
        subscription_transactions.platform,
        subscription_transactions.product_identifier

)

select * from final
