with

{% set get_first_subscription_date %}
    select {{ dbt.cast('cast(min(start_time) as date)', dbt.type_string()) }} from {{ ref('revenuecat_subscription_transactions') }}
{% endset %}
{%- set first_subscription_date = tasman_dbt_revenuecat.get_single_value(get_first_subscription_date, default="'2023-01-01'") -%}

date_spine as (

    {{ tasman_dbt_revenuecat.date_spine(
        datepart="day",
        start_date="'" + first_subscription_date + "'",
        end_date=tasman_dbt_revenuecat.current_utc_date()
       )
    }}

),

final as (

    select
        cast({{ dbt.date_trunc('day', 'date_day') }} as date) as date_day,
        cast({{ dbt.date_trunc('week', 'date_day') }} as date) as date_week,
        cast({{ dbt.date_trunc('month', 'date_day') }} as date) as date_month

    from
        date_spine

)

select * from final
