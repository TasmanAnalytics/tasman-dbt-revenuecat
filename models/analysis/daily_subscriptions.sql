with

subscription_transactions as (

    select * from {{ ref('revenuecat_subscription_transactions') }}

),

date_spine as (

    select * from {{ ref('date_spine') }}

),

final as (

    select
        date_spine.date_day,
        country_code,
        platform,
        product_identifier,
        count(distinct case when is_trial_period then store_transaction_id end) as trial_subscription_count,
        count(distinct case when not (is_trial_period) then store_transaction_id end) as paid_subscription_count

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

)

select * from final
