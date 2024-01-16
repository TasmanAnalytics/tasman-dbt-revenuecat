{{
    config(
        materialized='table',
    )
}}

with 

source as (

    select * from {{ source('revenuecat', 'transactions') }}

),

renamed as (

    select
        rc_original_app_user_id,
        rc_last_seen_app_user_id_alias,
        country,
        product_identifier,
        product_display_name,
        product_duration,
        start_time::timestamp_ntz as start_time,
        end_time::timestamp_ntz as expected_end_time,
        grace_period_end_time::timestamp_ntz as grace_period_end_time,
        effective_end_time::timestamp_ntz as effective_end_time,
        store,
        is_auto_renewable,
        is_trial_period,
        is_in_intro_offer_period,
        is_sandbox,
        price_in_usd,
        takehome_percentage,
        store_transaction_id,
        original_store_transaction_id,
        refunded_at::timestamp_ntz as refunded_at,
        unsubscribe_detected_at::timestamp_ntz as unsubscribe_detected_at,
        billing_issues_detected_at::timestamp_ntz as billing_issues_detected_at,
        purchased_currency,
        price_in_purchased_currency,
        entitlement_identifiers,
        renewal_number,
        is_trial_conversion,
        presented_offering,
        reserved_subscriber_attributes,
        custom_subscriber_attributes,
        platform,
        updated_at as valid_from,
        lead(updated_at) over (partition by store_transaction_id order by updated_at) as valid_to

    from source

)

select * from renamed