{{ config(materialized='table') }}

-- Current version of every complaint still present in the source, typed and renamed.
with versions as (
    select * from {{ ref('snap_complaints') }}
),

first_seen as (
    select
        complaint_id,
        min(dbt_valid_from) as first_valid_from
    from versions
    group by complaint_id
),

environment_start as (
    select min(dbt_valid_from) as first_snapshot_ts
    from versions
),

current_versions as (
    select * from versions
    where dbt_valid_to is null
)

select
    {{ safe_int('current_versions.complaint_id') }} as complaint_id,
    {{ safe_date('current_versions.date_received') }} as received_date,
    {{ safe_date('current_versions.date_sent_to_company') }} as sent_to_company_date,
    current_versions.product,
    current_versions.sub_product,
    current_versions.issue,
    current_versions.sub_issue,
    current_versions.company as company_name,
    {{ normalize_company_name('current_versions.company') }} as company_name_normalized,
    current_versions.state as state_code,
    current_versions.zip_code,
    current_versions.submitted_via,
    current_versions.company_response,
    current_versions.company_public_response,
    current_versions.timely_response = 'Yes' as is_timely_response,
    coalesce(current_versions.tags like '%Older American%', false) as is_older_american,
    coalesce(current_versions.tags like '%Servicemember%', false) as is_servicemember,
    -- Complaints in the first snapshot were not observed arriving, so they have no
    -- first-observed date.
    case
        when first_seen.first_valid_from = environment_start.first_snapshot_ts then null
        else cast(first_seen.first_valid_from as date)
    end as first_observed_date,
    first_seen.first_valid_from = environment_start.first_snapshot_ts as is_backfill
from current_versions
inner join first_seen
    on current_versions.complaint_id = first_seen.complaint_id
cross join environment_start
