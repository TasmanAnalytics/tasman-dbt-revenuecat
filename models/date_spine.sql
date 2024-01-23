with 

{% set get_first_subscription_date %}
    select min(start_time)::date::string from {{ source('revenuecat', 'transactions') }}
{% endset %}
{%- set first_subscription_date = dbt_utils.get_single_value(get_first_subscription_date, default="'2023-01-01'") -%}


date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" + first_subscription_date + "'",
        end_date="convert_timezone('UTC', current_timestamp)::date"
       )
    }}

),

final as (

	select 
		date_day,
		date_trunc(week, date_day) as date_week,
		date_trunc(month, date_day) as date_month

	from 
		date_spine

)

select * from final