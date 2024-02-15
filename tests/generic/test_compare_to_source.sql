{% test test_compare_to_source(model, column_name, source_model, source_date, source_metric, date_field, percent_mismatch_threshold = 0) %}

with src as (
    select
        {{ source_date }} as date,
        sum({{ source_metric }}) as measure_src

    from {{ source_model }}

    group by 1
),

new_model as (
    select
        {{ date_field }} as date,
        sum({{ column_name }}) as measure_comparison

    from {{ model }}

    group by date
),

comparison as (
    select
        src.date,
        src.measure_src,
        new_model.measure_comparison,
        case
            when src.measure_src = 0 then src.measure_src = new_model.measure_comparison
            when src.measure_src != 0 then abs(round((div0(new_model.measure_comparison, src.measure_src) - 1), 3)) <= ( {{  percent_mismatch_threshold  }} / 100.0 )
        else false
        end as is_match
    from 
        src
            left join new_model
                on src.date = new_model.date
)

select * from comparison where is_match = false


{% endtest %}