{% macro regexp_extract(column, pattern) %}
    {% if target.type == 'snowflake' %}
        regexp_substr({{ column }}, '{{ pattern }}')
    {% elif target.type == 'bigquery' %}
        regexp_extract({{ column }}, '{{ pattern }}')
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter type: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro json_extract(json_column, path) %}
    {% if target.type == 'snowflake' %}
        parse_json({{ json_column }}):{{ path }}
    {% elif target.type == 'bigquery' %}
        json_extract_scalar({{ json_column }}, '$.{{ path }}')
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter type: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro get_file_name() %}
    {% if target.type == 'snowflake' %}
    {% elif target.type == 'bigquery' %}
        , _file_name
    {% endif %}
{% endmacro %}

{% macro unix_seconds_to_timestamp(unix_seconds) %}
    {% if target.type == 'bigquery' %}
        timestamp_seconds({{ unix_seconds }})
    {% elif target.type == 'snowflake' %}
        {{ unix_seconds }}::timestamp_ntz
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro current_utc_date() %}
    {% if target.type == 'bigquery' %}
        current_date('UTC')
    {% elif target.type == 'snowflake' %}
        convert_timezone('UTC', current_timestamp)::date
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro select_star_exclude(relation, exclude_columns) %}
    {% if target.type == 'snowflake' %}
        * exclude ({{ exclude_columns | join(', ') }})
    {% elif target.type == 'bigquery' %}
        * except ({{ exclude_columns | join(', ') }})
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
    {% endif %}
{% endmacro %}


{% macro array_agg_distinct_no_nulls(column_name) %}
    {% if target.type == 'snowflake' %}
        array_agg(distinct {{ column_name }}) within group (order by {{ column_name }})
        where {{ column_name }} is not null
    {% elif target.type == 'bigquery' %}
        array_agg(distinct {{ column_name }} ignore nulls)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro current_pacific_timestamp() %}
    {% if target.type == 'bigquery' %}
        timestamp(datetime(current_timestamp(), 'America/Los_Angeles'))
    {% elif target.type == 'snowflake' %}
        convert_timezone('America/Los_Angeles', current_timestamp())::timestamp_ntz
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro array_unnest(array_column, alias) %}
    {% if target.type == 'snowflake' %}
        lateral flatten(input => {{ array_column }}) as {{ alias }},
        value as {{ alias }}_value
    {% elif target.type == 'bigquery' %}
        cross join unnest({{ array_column }}) as {{ alias }},
        {{ alias }} as {{ alias }}_value
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter type: " ~ target.type) }}
    {% endif %}
{% endmacro %}

{% macro flatten_arrays(relation, array_columns, value_columns) %}
    {% if target.type == 'snowflake' %}
        select
            {{ relation }}.*,
            {% for array_col, value_col in zip(array_columns, value_columns) %}
            flattened_{{ array_col }}.value as {{ value_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ relation }},
        {% for array_col in array_columns %}
        lateral flatten(input => {{ array_col }}, outer => true) as flattened_{{ array_col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    {% elif target.type == 'bigquery' %}
        select
            {{ relation }}.*,
            {% for array_col, value_col in zip(array_columns, value_columns) %}
            flattened_{{ array_col }} as {{ value_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ relation }}
        {% for array_col in array_columns %}
        cross join unnest({{ relation }}.{{ array_col }}) as flattened_{{ array_col }}
        {% endfor %}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported adapter type: " ~ target.type) }}
    {% endif %}
{% endmacro %}
