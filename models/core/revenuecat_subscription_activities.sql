with

subscription_transactions as (

    select * from {{ ref('stg_revenuecat_transactions') }} where valid_to is null

),

unpivoted_transactions as (

    select * from subscription_transactions

    -- the order of this unpivot is important. Where there is no gap between, the events are selected in order of priority.
    unpivot(activity_timestamp for timestamp_type in (
        start_time,
        refunded_at,
        unsubscribe_detected_at,
        billing_issues_detected_at,
        grace_period_end_time,
        effective_end_time
    ))
    
),

final as (

    select
        * exclude (timestamp_type),
        case
            when lower(timestamp_type) = 'start_time' then 'subscription_started'
            when lower(timestamp_type) = 'refunded_at' then 'subscription_refunded'
            when lower(timestamp_type) = 'unsubscribe_detected_at' then 'subscription_cancelled'
            when lower(timestamp_type) = 'billing_issues_detected_at' then 'billing_issue_detected'
            when lower(timestamp_type) = 'grace_period_end_time' then 'grace_period_ended'
            when lower(timestamp_type) = 'effective_end_time' then 'subscription_ended'
            else timestamp_type
        end as activity
    
    from
        unpivoted_transactions

)

select * from final
    