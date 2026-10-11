{#-
    SCD2 validity must come from the snapshot date, not the wall clock, so that a
    re-run or a replay gives identical history. dbt stamps closed and deleted rows
    with snapshot_get_time(); these overrides pin it to the snapshot being applied.
-#}
{% macro snapshot_ts() -%}
    cast('{{ var("snapshot_date") }} 00:00:00' as timestamp)
{%- endmacro %}

{% macro snapshot_get_time() -%}
    {{ snapshot_ts() }}
{%- endmacro %}

{% macro default__snapshot_get_time() -%}
    {{ snapshot_ts() }}
{%- endmacro %}

{% macro duckdb__snapshot_get_time() -%}
    {{ snapshot_ts() }}
{%- endmacro %}

{% macro bigquery__snapshot_get_time() -%}
    {{ snapshot_ts() }}
{%- endmacro %}
