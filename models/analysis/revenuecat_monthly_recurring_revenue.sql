with

subscription_transactions as (
    select * from {{ ref('revenuecat_subscription_transactions') }} 
    where 
        valid_to is null 
        and is_trial_period = false 
        and ownership_type != 'FAMILY_SHARED' 
        and datediff('s', start_time, expected_end_time)::float > 0 
        and store != 'promotional'
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
        {{ tasman_dbt_revenuecat.generate_surrogate_key([
            'date_spine.date_month',
            'transaction_monthly_revenue.country_code',
            'transaction_monthly_revenue.platform',
            'transaction_monthly_revenue.product_identifier'
        ])}} as row_id,
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier,
        sum(transaction_monthly_revenue.mrr_in_usd) as mrr_in_usd

    from
        date_spine

    left join
        transaction_monthly_revenue
        on transaction_monthly_revenue.start_time::date <= last_day(date_month, month)
        and case 
                when transaction_monthly_revenue.effective_end_time >= convert_timezone('America/Los_Angeles', 'UTC', SYSDATE()::date) -- Assumes that RevenueCat runs their data model at this time. This gives an 99.9% accurate MRR number for the current month & prior months
                then transaction_monthly_revenue.effective_end_time::date > date_spine.date_month
                else transaction_monthly_revenue.effective_end_time::date > last_day(date_spine.date_month, month)
            end


    group by
        date_spine.date_month,
        transaction_monthly_revenue.country_code,
        transaction_monthly_revenue.platform,
        transaction_monthly_revenue.product_identifier

)

select * from final
