{# Partitioning and clustering are BigQuery features; DuckDB rejects the settings. #}
{% if target.type == 'bigquery' %}
    {{
        config(
            partition_by={'field': 'received_date', 'data_type': 'date', 'granularity': 'month'},
            cluster_by=['company_key', 'fraud_category'],
        )
    }}
{% endif %}

-- One row per complaint currently in the CFPB database.
select
    cast(complaint_id as int64) as complaint_id,
    cast(received_date as date) as received_date,
    {{ date_key('received_date') }} as received_date_key,
    {{ date_key('sent_to_company_date') }} as sent_to_company_date_key,
    cast(company_key as string) as company_key,
    cast(product_key as string) as product_key,
    cast(issue_key as string) as issue_key,
    cast(geography_key as string) as geography_key,
    cast(zip_code as string) as zip_code,
    cast(fraud_category as string) as fraud_category,
    cast(is_fraud_related as bool) as is_fraud_related,
    cast(submitted_via as string) as submitted_via,
    cast(company_response as string) as company_response,
    cast(is_timely_response as bool) as is_timely_response,
    cast(is_older_american as bool) as is_older_american,
    cast(is_servicemember as bool) as is_servicemember,
    cast({{ days_between('received_date', 'sent_to_company_date') }} as int64) as days_to_send,
    cast(first_observed_date as date) as first_observed_date,
    cast(is_backfill as bool) as is_backfill
from {{ ref('int_complaints_classified') }}
