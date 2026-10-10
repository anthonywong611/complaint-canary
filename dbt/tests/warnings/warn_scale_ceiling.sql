{{ config(severity='warn', meta={'dagster': {'ref': {'name': 'stg_snapshot_ledger'}}}) }}

-- Early warning that the full-snapshot design is nearing its limits: the monthly
-- BigQuery scan approaching the free tier, or the row count approaching the ceiling.
with recent_scan as (
    select coalesce(sum(bytes_billed), 0) as bytes_billed_30d
    from {{ source('ops', 'load_audit') }}
    where snapshot_date >= {{ add_days(snapshot_date(), -30) }}
),

latest_rows as (
    select coalesce(max(row_count), 0) as row_count
    from {{ ref('stg_snapshot_ledger') }}
)

select
    recent_scan.bytes_billed_30d,
    latest_rows.row_count
from recent_scan
cross join latest_rows
where
    recent_scan.bytes_billed_30d > {{ var('scale_warn_bytes_30d') }}
    or latest_rows.row_count > {{ var('scale_warn_rows') }}
