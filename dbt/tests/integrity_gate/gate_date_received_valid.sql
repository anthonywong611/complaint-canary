{{ config(tags=['integrity_gate']) }}

-- date_received must parse as a date and cannot be later than the snapshot.
select
    complaint_id,
    date_received
from {{ source('raw', 'complaints_snapshot') }}
where
    snapshot_date = {{ snapshot_date() }}
    and (
        {{ safe_date('date_received') }} is null
        or {{ safe_date('date_received') }} > {{ snapshot_date() }}
    )
