with

subscription_activities as (

    select * from {{ ref('revenuecat_subscription_activities') }}

),

distinct_users as (

    select
        rc_original_app_user_id,
        first_value(country_code) over (partition by rc_original_app_user_id order by activity_timestamp) as country_code,
        first_value(platform) over (partition by rc_original_app_user_id order by activity_timestamp) as platform,
        activity_timestamp as start_timestamp

    from
        subscription_activities

    qualify
        row_number() over (partition by rc_original_app_user_id order by start_timestamp) = 1

),

trial_time as (

    select
        rc_original_app_user_id,
        min(activity_timestamp) as trial_timestamp

    from
        subscription_activities

    where
        renewal_number = 1
        and is_trial_period = true

    group by
        rc_original_app_user_id

),

new_paid_time as (

    select
        rc_original_app_user_id,
        min(activity_timestamp) as new_paid_timestamp

    from
        subscription_activities

    where
        renewal_number = 1
        and is_trial_period = false

    group by
        rc_original_app_user_id

),

conversion_time as (

    select
        rc_original_app_user_id,
        min(activity_timestamp) as conversion_timestamp

    from
        subscription_activities

    where
        renewal_number = 2
        and is_trial_conversion = true

    group by
        rc_original_app_user_id

),

final as (

    select
        distinct_users.rc_original_app_user_id,
        distinct_users.country_code,
        distinct_users.platform,
        distinct_users.start_timestamp,
        trial_time.trial_timestamp,
        coalesce(trial_time.trial_timestamp is not null, false) as is_new_trial,
        new_paid_time.new_paid_timestamp,
        coalesce(new_paid_time.new_paid_timestamp is not null, false) as is_new_paid,
        conversion_time.conversion_timestamp,
        coalesce(conversion_time.conversion_timestamp is not null, false) as is_new_conversion

    from
        distinct_users

    left join
        trial_time
        on distinct_users.rc_original_app_user_id = trial_time.rc_original_app_user_id

    left join
        new_paid_time
        on distinct_users.rc_original_app_user_id = new_paid_time.rc_original_app_user_id

    left join
        conversion_time
        on distinct_users.rc_original_app_user_id = conversion_time.rc_original_app_user_id

)

select * from final
