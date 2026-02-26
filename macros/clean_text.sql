{% macro clean_text(column_name) %}

case
    when {{ column_name }} is null then null

    when lower(trim({{ column_name }})) in (
        '',
        'null',
        'n/a',
        'na',
        'unknown',
        '-'
    )
    then null

    else trim({{ column_name }})
end

{% endmacro %}