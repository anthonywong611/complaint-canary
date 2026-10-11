{{ config(meta={'dagster': {'ref': {'name': 'fct_complaint_status_history'}}}) }}

-- Change history only exists from the first snapshot this environment applied.
-- A row dated earlier would claim to have observed something it could not have.
with environment_start as (
    select min(snapshot_date) as first_snapshot_date
    from {{ ref('stg_snapshot_ledger') }}
)

select
    history.complaint_id,
    history.valid_from
from {{ ref('fct_complaint_status_history') }} as history
cross join environment_start
where history.valid_from < environment_start.first_snapshot_date
