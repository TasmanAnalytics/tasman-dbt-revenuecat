with

subscription_transactions as (
    select * from {{ ref('revenuecat_subscription_transactions') }} where valid_to is null
),

date_spine as (
    select distinct date_month from {{ ref('revenuecat_date_spine') }}
),

transaction_monthly_revenue as (

    select
        store_transaction_id,
        country_code,
        platform,
        product_identifier,
        start_time,
        effective_end_time,
        mrr_in_usd

    from
        subscription_transactions

),

final as (

    select
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier,
        sum(transaction_monthly_revenue.mrr_in_usd) as mrr_in_usd

    from
        date_spine

    left join
        transaction_monthly_revenue
        on date_spine.date_month >= transaction_monthly_revenue.start_time::date
        and last_day(date_spine.date_month, month) <= last_day(transaction_monthly_revenue.effective_end_time::date, month)

    group by
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier

)

select * from final
