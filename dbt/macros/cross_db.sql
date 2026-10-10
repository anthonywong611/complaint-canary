{#-
    The few places where BigQuery and DuckDB differ. Everything else in the project
    is written in the SQL both accept.
-#}

{% macro snapshot_date() -%}
    cast('{{ var("snapshot_date") }}' as date)
{%- endmacro %}

{% macro float_type() -%}
    {%- if target.type == 'bigquery' -%}float64{%- else -%}double{%- endif -%}
{%- endmacro %}

{% macro days_between(start_date, end_date) -%}
    {%- if target.type == 'bigquery' -%}
        date_diff({{ end_date }}, {{ start_date }}, day)
    {%- else -%}
        date_diff('day', {{ start_date }}, {{ end_date }})
    {%- endif -%}
{%- endmacro %}

{% macro add_days(date_expression, days) -%}
    {%- if target.type == 'bigquery' -%}
        date_add({{ date_expression }}, interval {{ days }} day)
    {%- else -%}
        cast({{ date_expression }} + cast({{ days }} as integer) as date)
    {%- endif -%}
{%- endmacro %}

{% macro week_start(date_expression) -%}
    {%- if target.type == 'bigquery' -%}
        date_trunc({{ date_expression }}, isoweek)
    {%- else -%}
        cast(date_trunc('week', {{ date_expression }}) as date)
    {%- endif -%}
{%- endmacro %}

{% macro safe_date(expression) -%}
    {%- if target.type == 'bigquery' -%}
        safe_cast({{ expression }} as date)
    {%- else -%}
        try_cast({{ expression }} as date)
    {%- endif -%}
{%- endmacro %}

{% macro safe_int(expression) -%}
    {%- if target.type == 'bigquery' -%}
        safe_cast({{ expression }} as int64)
    {%- else -%}
        try_cast({{ expression }} as int64)
    {%- endif -%}
{%- endmacro %}

{% macro hash_text(expression) -%}
    {%- if target.type == 'bigquery' -%}
        to_hex(md5({{ expression }}))
    {%- else -%}
        md5({{ expression }})
    {%- endif -%}
{%- endmacro %}

{% macro hash_columns(columns) -%}
    {%- set parts = [] -%}
    {%- for column in columns -%}
        {%- do parts.append("coalesce(cast(" ~ column ~ " as string), '')") -%}
    {%- endfor -%}
    {{ hash_text(parts | join(" || '|' || ")) }}
{%- endmacro %}

{% macro regex_replace(expression, pattern, replacement) -%}
    {%- if target.type == 'bigquery' -%}
        regexp_replace({{ expression }}, '{{ pattern }}', '{{ replacement }}')
    {%- else -%}
        regexp_replace({{ expression }}, '{{ pattern }}', '{{ replacement }}', 'g')
    {%- endif -%}
{%- endmacro %}

{% macro regex_match(expression, pattern) -%}
    {%- if target.type == 'bigquery' -%}
        regexp_contains({{ expression }}, "{{ pattern }}")
    {%- else -%}
        regexp_matches({{ expression }}, '{{ pattern | replace("'", "''") }}')
    {%- endif -%}
{%- endmacro %}

{% macro date_key(date_expression) -%}
    cast(
        extract(year from {{ date_expression }}) * 10000
        + extract(month from {{ date_expression }}) * 100
        + extract(day from {{ date_expression }})
        as int64
    )
{%- endmacro %}

{% macro no_rows() -%}
    select 1 as passed from (select 1 as passed) as nothing where 1 = 0
{%- endmacro %}
