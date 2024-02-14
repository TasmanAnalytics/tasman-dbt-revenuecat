{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}

with

source as (

    select * from {{ source('revenuecat', 'transactions') }}
    where 1=1
    {% if var('revenuecat_filter') %}
    and {{ var('revenuecat_filter') }}
    {% endif %}
    {% if is_incremental() %}
    and regexp_substr(_file_name, '[0-9]{10}')::timestamp_ntz >= (select max(_exported_at) from {{ this }})
    {% endif %}

),

deduplicate as (

    select * from source
    qualify row_number() over (partition by store_transaction_id, updated_at order by regexp_substr(_file_name, '[0-9]{10}')::timestamp_ntz desc) = 1

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['store_transaction_id', 'updated_at'])}} as id,
        rc_original_app_user_id,
        rc_last_seen_app_user_id_alias,
        country as country_code,
        product_identifier,
        start_time::timestamp_ntz as start_time,
        end_time::timestamp_ntz as expected_end_time,
        store,
        is_auto_renewable::boolean as is_auto_renewable,
        is_trial_period::boolean as is_trial_period,
        is_in_intro_offer_period::boolean as is_in_intro_offer_period,
        is_sandbox::boolean as is_sandbox,

        price_in_usd,
        price_in_usd * commission_percentage as commission_in_usd,
        price_in_usd * tax_percentage as estimated_tax_in_usd,
        price_in_usd - commission_in_usd - estimated_tax_in_usd as proceeds_in_usd,
        -- Documentation for normalizing mrr_usd: https://docs.revenuecat.com/docs/monthly-recurring-revenue-mrr_usd-chart
        case
            when product_duration = 'P1M' then price_in_usd
            when product_duration = 'P1Y' then price_in_usd / 12
            when product_duration = 'P1W' then price_in_usd * 4
            else 0
        end as mrr_usd,
        takehome_percentage,

        store_transaction_id,
        original_store_transaction_id,

        refunded_at::timestamp_ntz as refunded_at,
        refunded_at is not null as is_refunded,

        unsubscribe_detected_at::timestamp_ntz as unsubscribe_detected_at,
        billing_issues_detected_at::timestamp_ntz as billing_issues_detected_at,

        purchased_currency,
        price_in_purchased_currency,
        price_in_purchased_currency * commission_percentage as commission_in_purchased_currency,
        price_in_purchased_currency * tax_percentage as estimated_tax_in_purchased_currency,
        price_in_purchased_currency - commission_in_purchased_currency - estimated_tax_in_purchased_currency as proceeds_in_purchased_currency,
        
        entitlement_identifiers,
        renewal_number,
        is_trial_conversion::boolean as is_trial_conversion,
        store_transaction_id = original_store_transaction_id or is_trial_conversion::boolean as is_new_revenue,
        
        presented_offering,
        reserved_subscriber_attributes,
        custom_subscriber_attributes,
        platform,
        tax_percentage,
        commission_percentage,
        case
            when grace_period_end_time is not null then grace_period_end_time::timestamp_ntz
            else effective_end_time::timestamp_ntz
        end as effective_end_time,
        grace_period_end_time::timestamp_ntz as grace_period_end_time,
        grace_period_end_time is not null as is_grace_period,
        ownership_type,
        country_source,
        experiment_id,
        experiment_variant,
        purchase_price_in_usd,
        purchase_price_in_purchased_currency,
        product_display_name,
        product_duration,
        --offer,
        --offer_type,
        --first_seen_time,
        --auto_resume_time,

        {%- if var('revenuecat_custom_subscriber_attributes') %}
            {%- for key, value in var('revenuecat_custom_subscriber_attributes').items() %}
            parse_json(custom_subscriber_attributes):{{ key }} as {{ value }},
            {%- endfor %}
        {%- endif %}

        updated_at as valid_from,
        lead(updated_at) over (partition by store_transaction_id order by updated_at) as valid_to,
        regexp_substr(_file_name, '[0-9]{10}')::timestamp_ntz as _exported_at

    from deduplicate

)

select * from renamed
