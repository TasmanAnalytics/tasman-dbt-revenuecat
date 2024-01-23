with

final as (

    select * from {{ ref('stg_revenuecat_transactions') }} where valid_to is null

)

select * from final