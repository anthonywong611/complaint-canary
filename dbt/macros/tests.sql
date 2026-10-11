{% test unique_combination(model, columns) %}
    select
        {{ columns | join(', ') }},
        count(*) as occurrences
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}

{% test at_least(model, column_name, minimum) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ minimum }}
{% endtest %}
