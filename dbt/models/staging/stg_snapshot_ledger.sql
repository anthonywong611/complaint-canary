-- Snapshots that were applied to history, each with the one applied before it.
select
    snapshot_date,
    state,
    row_count,
    lag(snapshot_date) over (order by snapshot_date) as previous_snapshot_date
from {{ source('ops', 'snapshot_ledger') }}
where state in ('validated', 'applied', 'published')
