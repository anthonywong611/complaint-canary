{{ config(tags=['integrity_gate']) }}

-- Compared with the last applied snapshot, the row count may not fall by more than
-- 0.5% (a truncated file) or grow by more than 1% per elapsed day (a duplicated one).
{% if var('gate_override') %}
    {{ no_rows() }}
{% else %}
    with current_snapshot as (
        select count(*) as row_count
        from {{ source('raw', 'complaints_snapshot') }}
        where snapshot_date = {{ snapshot_date() }}
    ),

    applied as (
        select
            snapshot_date,
            row_count,
            row_number() over (order by snapshot_date desc) as recency_rank
        from {{ source('ops', 'snapshot_ledger') }}
        where
            state in ('applied', 'published')
            and snapshot_date < {{ snapshot_date() }}
    ),

    previous_snapshot as (
        select
            snapshot_date,
            row_count
        from applied
        where recency_rank = 1
    )

    select
        current_snapshot.row_count,
        previous_snapshot.row_count as previous_row_count,
        previous_snapshot.snapshot_date as previous_snapshot_date
    from current_snapshot
    cross join previous_snapshot
    where
        current_snapshot.row_count < previous_snapshot.row_count * {{ var('gate_min_row_ratio') }}
        or current_snapshot.row_count > previous_snapshot.row_count * (
            1 + {{ var('gate_max_daily_growth') }}
            * {{ days_between('previous_snapshot.snapshot_date', snapshot_date()) }}
        )
{% endif %}
