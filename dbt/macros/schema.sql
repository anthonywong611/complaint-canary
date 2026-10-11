{#- Each layer is its own schema (BigQuery dataset), with an optional environment prefix. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = env_var('CANARY_DATASET_PREFIX', '') -%}
    {%- if custom_schema_name is none -%}
        {{ prefix }}staging
    {%- else -%}
        {{ prefix }}{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
