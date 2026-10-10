{{ config(severity='warn', meta={'dagster': {'ref': {'name': 'dim_company'}}}) }}

-- Among the highest-volume companies of the last 90 days, too much volume belongs to
-- companies that are neither matched to an FDIC institution nor listed in the
-- company_overrides seed. Add them to the seed as bank, credit_union or non_bank.
with recent as (
    select
        company_key,
        count(*) as complaint_count
    from {{ ref('fct_complaints') }}
    where received_date >= {{ add_days(snapshot_date(), -90) }}
    group by company_key
),

ranked as (
    select
        recent.company_key,
        recent.complaint_count,
        companies.entity_type,
        row_number() over (order by recent.complaint_count desc, recent.company_key asc) as volume_rank
    from recent
    inner join {{ ref('dim_company') }} as companies
        on recent.company_key = companies.company_key
),

summary as (
    select
        sum(complaint_count) as top_volume,
        sum(case when entity_type = 'unreviewed' then complaint_count else 0 end) as unreviewed_volume
    from ranked
    where volume_rank <= {{ var('unreviewed_top_n') }}
)

select
    top_volume,
    unreviewed_volume
from summary
where unreviewed_volume > top_volume * {{ var('unreviewed_warn_share') }}
