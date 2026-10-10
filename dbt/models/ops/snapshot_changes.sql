-- What each applied snapshot changed: complaints added, changed and removed.
with history as (
    select * from {{ ref('fct_complaint_status_history') }}
),

arrivals as (
    select
        valid_from as snapshot_date,
        sum(case when change_type in ('backfill', 'new', 'reappeared') then 1 else 0 end) as new_count,
        sum(case when change_type in ('status', 'attribute', 'status_and_attribute') then 1 else 0 end)
            as changed_count,
        0 as deleted_count
    from history
    group by valid_from
),

removals as (
    select
        valid_to as snapshot_date,
        0 as new_count,
        0 as changed_count,
        count(*) as deleted_count
    from history
    where is_deleted
    group by valid_to
),

combined as (
    select * from arrivals
    union all
    select * from removals
)

select
    snapshot_date,
    cast(sum(new_count) as int64) as new_count,
    cast(sum(changed_count) as int64) as changed_count,
    cast(sum(deleted_count) as int64) as deleted_count
from combined
group by snapshot_date
