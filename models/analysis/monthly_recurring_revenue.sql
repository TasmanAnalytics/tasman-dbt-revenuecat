with

subscription_transactions as (

    select * from {{ ref('revenuecat_subscription_transactions') }}

),

date_spine as (

    select distinct date_month from {{ ref('date_spine') }}

),

transaction_monthly_revenue as (

    select
        store_transaction_id,
        country_code,
        platform,
        product_identifier,
        start_time,
        effective_end_time,
        -- Documentation for normalizing mrr: https://docs.revenuecat.com/docs/monthly-recurring-revenue-mrr-chart
        case
            when product_duration = 'P1M' then price_in_usd
            when product_duration = 'P1Y' then price_in_usd / 12
            when product_duration = 'P1W' then price_in_usd * 4
            else 0
        end as monthly_revenue

    from
        subscription_transactions

),

final as (

    select
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier,
        sum(transaction_monthly_revenue.monthly_revenue) as mrr

    from
        date_spine

    left join
        transaction_monthly_revenue
        on last_day(date_spine.date_month, month) >= transaction_monthly_revenue.start_time::date
        and last_day(date_spine.date_month, month) < transaction_monthly_revenue.effective_end_time::date

    group by
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier

)

select * from final
