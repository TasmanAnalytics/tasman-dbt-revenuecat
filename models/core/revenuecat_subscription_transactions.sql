{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_row_id',
    merge_update_columns = ['valid_to'],
    )
}}

with 
{% if is_incremental() %} 
merge_identifiers as (
  select distinct store_transaction_id 
        from {{ source('revenuecat', 'transactions') }}
        where {{ tasman_dbt_revenuecat.unix_seconds_to_timestamp(dbt.cast(regexp_extract('_file_name', '[0-9]{10}'), dbt.type_int())) }} > (
            select max(_exported_at) 
            from {{ this }}
        )
),
{% endif %}

source as (
    select 
        * 
        {{ tasman_dbt_revenuecat.get_file_name() }}
    from 
        {{ source('revenuecat', 'transactions') }}

    where 1=1
        {% if var('revenuecat_filter') %}
        and {{ var('revenuecat_filter') }}
        {% endif %}
        {% if is_incremental() %} 
        and store_transaction_id in (
                select store_transaction_id 
                from merge_identifiers
        )
        {% endif %}

),

deduplicate as (
    select * from source
    qualify
        row_number() over (partition by store_transaction_id, {{ dbt.date_trunc('microsecond', dbt.cast('updated_at', dbt.type_timestamp())) }} order by {{ tasman_dbt_revenuecat.unix_seconds_to_timestamp(dbt.cast(regexp_extract('_file_name', '[0-9]{10}'), dbt.type_int())) }} desc) = 1
),

renamed as (
    select
        {{ tasman_dbt_revenuecat.generate_surrogate_key(["store_transaction_id", dbt.cast(dbt.date_trunc('microsecond', 'updated_at'), dbt.type_string())]) }} as transaction_row_id,
        rc_original_app_user_id,
        rc_last_seen_app_user_id_alias,
        country as country_code,
        product_identifier,
        {{ dbt.cast('start_time', dbt.type_timestamp()) }} as start_time,
        {{ dbt.cast('end_time', dbt.type_timestamp()) }} as expected_end_time,
        store,
        {{ dbt.cast('is_auto_renewable', dbt.type_boolean()) }} as is_auto_renewable,
        {{ dbt.cast('is_trial_period', dbt.type_boolean()) }} as is_trial_period,
        {{ dbt.cast('is_in_intro_offer_period', dbt.type_boolean()) }} as is_in_intro_offer_period,
        {{ dbt.cast('is_sandbox', dbt.type_boolean()) }} as is_sandbox,
        price_in_usd,
        price_in_usd * commission_percentage as commission_in_usd,
        price_in_usd * tax_percentage as estimated_tax_in_usd,
        price_in_usd - (price_in_usd * commission_percentage) - (price_in_usd * tax_percentage) as proceeds_in_usd,
        store_transaction_id,
        original_store_transaction_id,
        {{ dbt.cast('refunded_at', dbt.type_timestamp()) }} as refunded_at,
        refunded_at is not null as is_refunded,
        {{ dbt.cast('unsubscribe_detected_at', dbt.type_timestamp()) }} as unsubscribe_detected_at,
        {{ dbt.cast('billing_issues_detected_at', dbt.type_timestamp()) }} as billing_issues_detected_at,
        purchased_currency,
        price_in_purchased_currency,
        price_in_purchased_currency * commission_percentage as commission_in_purchased_currency,
        price_in_purchased_currency * tax_percentage as estimated_tax_in_purchased_currency,
        price_in_purchased_currency - (price_in_purchased_currency * commission_percentage) - (price_in_purchased_currency * tax_percentage) as proceeds_in_purchased_currency,
        entitlement_identifiers,
        renewal_number,
        {{ dbt.cast('is_trial_conversion', dbt.type_boolean()) }} as is_trial_conversion,
        store_transaction_id = original_store_transaction_id or {{ dbt.cast('is_trial_conversion', dbt.type_boolean()) }} as is_new_revenue,
        presented_offering,
        reserved_subscriber_attributes,
        custom_subscriber_attributes,
        platform,
        tax_percentage,
        commission_percentage,
        {{ dbt.cast('effective_end_time', dbt.type_timestamp()) }} as effective_end_time,
        {{ dbt.cast('grace_period_end_time', dbt.type_timestamp()) }} as grace_period_end_time,
        grace_period_end_time is not null as is_grace_period,
        ownership_type,
        country_source,
        experiment_id,
        experiment_variant,
        purchase_price_in_usd,
        purchase_price_in_purchased_currency,
        product_display_name,
        product_duration,
        {%- if var('revenuecat_version') > 4 %}
        offer,
        offer_type,
        first_seen_time,
        auto_resume_time,
        {%- endif %}

        {%- if var('revenuecat_custom_subscriber_attributes') %}
            {%- for key, value in var('revenuecat_custom_subscriber_attributes').items() %}
            {{ json_extract('custom_subscriber_attributes', key) }} as {{ value }},
            {%- endfor %}
        {%- endif %}

        {{ dbt.cast(dbt.cast(dbt.date_trunc('microsecond', 'updated_at'), dbt.type_string()), dbt.type_timestamp()) }} as valid_from,
        lead({{ dbt.cast(dbt.cast(dbt.date_trunc('microsecond', 'updated_at'), dbt.type_string()), dbt.type_timestamp()) }}) over (partition by store_transaction_id order by {{ dbt.cast(dbt.cast(dbt.date_trunc('microsecond', 'updated_at'), dbt.type_string()), dbt.type_timestamp()) }}) as valid_to,
        {{ tasman_dbt_revenuecat.unix_seconds_to_timestamp(dbt.cast(regexp_extract('_file_name', '[0-9]{10}'), dbt.type_int())) }} as _exported_at
    from deduplicate
),

mrr_calculations as (
    select
        *,
        case 
            when effective_end_time is not null then
                case 
                    /* handle cases where product_duration cannot be used for the transaction first */
                    when (is_in_intro_offer_period = true or product_duration is null) 
                        then 
                            case
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 0 and 1 then round(30 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} = 3 then round(10 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 6 and 8 then round(4 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 12 and 16 then round(2 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 27 and 33 then round(1 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 58 and 62 then round(0.5 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 88 and 95 then round(0.333333 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 179 and 185 then round(0.1666666 * price_in_usd, 2)
                                when {{ datediff("start_time", "expected_end_time", "day") }} between 363 and 375 then round(0.08333 * price_in_usd, 2)
                                else round(((28 / ({{ datediff("start_time", "expected_end_time", "second") }} / (24 * 3600))) * price_in_usd), 2)
                            end
                        /* then handle cases where product_duration can be used */
                    when product_duration = 'P1D' then round(30 * price_in_usd, 2)
                    when product_duration = 'P3D' then round(10 * price_in_usd, 2)
                    when product_duration = 'P7D' then round(4 * price_in_usd, 2)
                    when product_duration = 'P1W' then round(4 * price_in_usd, 2)
                    when product_duration = 'P2W' then round(2 * price_in_usd, 2)
                    when product_duration = 'P4W' then round(1 * price_in_usd, 2)
                    when product_duration = 'P1M' then round(1 * price_in_usd, 2)
                    when product_duration = 'P2M' then round(0.5 * price_in_usd, 2)
                    when product_duration = 'P3M' then round(0.333333 * price_in_usd, 2)
                    when product_duration = 'P6M' then round(0.1666666 * price_in_usd, 2)
                    when product_duration = 'P12M' then round(0.08333 * price_in_usd, 2)
                    when product_duration = 'P1Y' then round(0.08333 * price_in_usd, 2)
                    else round(((28 / ({{ datediff("start_time", "expected_end_time", "second") }} / (24 * 3600))) * price_in_usd), 2)
                end
        end as mrr_in_usd
    from renamed
)

select
    transaction_row_id,
    rc_original_app_user_id,
    rc_last_seen_app_user_id_alias,
    country_code,
    product_identifier,
    start_time,
    expected_end_time,
    store,
    is_auto_renewable,
    is_trial_period,
    is_in_intro_offer_period,
    is_sandbox,
    price_in_usd,
    commission_in_usd,
    estimated_tax_in_usd,
    proceeds_in_usd,
    store_transaction_id,
    original_store_transaction_id,
    refunded_at,
    is_refunded,
    unsubscribe_detected_at,
    billing_issues_detected_at,
    purchased_currency,
    price_in_purchased_currency,
    commission_in_purchased_currency,
    estimated_tax_in_purchased_currency,
    proceeds_in_purchased_currency,
    entitlement_identifiers,
    renewal_number,
    is_trial_conversion,
    is_new_revenue,
    presented_offering,
    reserved_subscriber_attributes,
    custom_subscriber_attributes,
    platform,
    tax_percentage,
    commission_percentage,
    effective_end_time,
    grace_period_end_time,
    is_grace_period,
    ownership_type,
    country_source,
    experiment_id,
    experiment_variant,
    purchase_price_in_usd,
    purchase_price_in_purchased_currency,
    product_display_name,
    product_duration,
    {%- if var('revenuecat_version') > 4 %}
    offer,
    offer_type,
    first_seen_time,
    auto_resume_time,
    {%- endif %}
    {%- if var('revenuecat_custom_subscriber_attributes') %}
        {%- for key, value in var('revenuecat_custom_subscriber_attributes').items() %}
        {{ value }},
        {%- endfor %}
    {%- endif %}
    valid_from,
    valid_to,
    _exported_at,
    mrr_in_usd
from mrr_calculations
