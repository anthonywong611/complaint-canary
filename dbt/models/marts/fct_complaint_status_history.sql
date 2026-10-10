-- Every observed version of every complaint. valid_from and valid_to are the dates
-- of the snapshots in which a change was seen; the change itself happened some time
-- after previous_snapshot_date.
with versions as (
    select
        *,
        lag(status_hash) over (partition by complaint_id order by valid_from) as previous_status_hash,
        lag(attribute_hash) over (partition by complaint_id order by valid_from) as previous_attribute_hash,
        lag(valid_to) over (partition by complaint_id order by valid_from) as previous_valid_to,
        lead(valid_from) over (partition by complaint_id order by valid_from) as next_valid_from
    from {{ ref('stg_complaint_versions') }}
),

ledger as (
    select * from {{ ref('stg_snapshot_ledger') }}
),

environment_start as (
    select min(snapshot_date) as first_snapshot_date
    from ledger
)

select
    cast(versions.complaint_id as int64) as complaint_id,
    cast(versions.valid_from as date) as valid_from,
    cast(versions.valid_to as date) as valid_to,
    cast(ledger.previous_snapshot_date as date) as previous_snapshot_date,
    cast(
        case
            when versions.previous_status_hash is null and versions.valid_from = environment_start.first_snapshot_date
                then 'backfill'
            when versions.previous_status_hash is null then 'new'
            when versions.previous_valid_to < versions.valid_from then 'reappeared'
            when
                versions.previous_status_hash <> versions.status_hash
                and versions.previous_attribute_hash <> versions.attribute_hash
                then 'status_and_attribute'
            when versions.previous_status_hash <> versions.status_hash then 'status'
            else 'attribute'
        end as string
    ) as change_type,
    cast(versions.company_response as string) as company_response,
    cast(versions.company_public_response as string) as company_public_response,
    -- The complaint left the source: its last version is closed and nothing follows it.
    cast(versions.valid_to is not null and versions.next_valid_from is null as bool) as is_deleted
from versions
left join ledger
    on versions.valid_from = ledger.snapshot_date
cross join environment_start
