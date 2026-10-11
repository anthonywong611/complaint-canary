{{ config(tags=['integrity_gate']) }}

-- Complaints in history but missing from the new snapshot would be closed as deleted.
-- More than 0.5% missing means the file is incomplete, not that CFPB removed them.
-- The history table is looked up directly, not through ref(), so that this test runs
-- before the snapshot is applied.
{% set history = none %}
{% if execute and not var('gate_override') %}
    {% set history = adapter.get_relation(
        database=target.database,
        schema=generate_schema_name('snapshots', none) | trim,
        identifier='snap_complaints'
    ) %}
{% endif %}

{% if history is none %}
    {{ no_rows() }}
{% else %}
    with active as (
        select complaint_id
        from {{ history }}
        where dbt_valid_to is null
    ),

    incoming as (
        select complaint_id
        from {{ source('raw', 'complaints_snapshot') }}
        where snapshot_date = {{ snapshot_date() }}
    ),

    compared as (
        select
            count(*) as active_count,
            sum(case when incoming.complaint_id is null then 1 else 0 end) as missing_count
        from active
        left join incoming
            on active.complaint_id = incoming.complaint_id
    )

    select
        active_count,
        missing_count
    from compared
    where missing_count > active_count * {{ var('gate_max_deleted_ratio') }}
{% endif %}
