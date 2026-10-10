{{ config(tags=['integrity_gate']) }}

-- The snapshot being applied must contain rows. An empty partition would otherwise
-- look like every complaint had been deleted.
with partition_size as (
    select count(*) as row_count
    from {{ source('raw', 'complaints_snapshot') }}
    where snapshot_date = {{ snapshot_date() }}
)

select row_count
from partition_size
where row_count = 0
