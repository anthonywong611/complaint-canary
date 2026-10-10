{{ config(tags=['integrity_gate']) }}

-- complaint_id must be present, numeric and unique within the snapshot.
with ids as (
    select
        complaint_id,
        count(*) as occurrences
    from {{ source('raw', 'complaints_snapshot') }}
    where snapshot_date = {{ snapshot_date() }}
    group by complaint_id
)

select
    complaint_id,
    occurrences
from ids
where
    complaint_id is null
    or {{ safe_int('complaint_id') }} is null
    or occurrences > 1
