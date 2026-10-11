-- FDIC institutions grouped under their holding company. Complaints name either the
-- bank or its parent, so both names are offered for matching, with the group's assets.
with institutions as (
    select
        *,
        coalesce(holding_company_name_normalized, 'CERT ' || cast(cert as string)) as group_key
    from {{ ref('stg_fdic_institutions') }}
),

group_totals as (
    select
        group_key,
        sum(total_assets_usd) as group_assets_usd
    from institutions
    group by group_key
),

name_counts as (
    select
        institution_name_normalized,
        count(*) as institutions_with_name
    from institutions
    group by institution_name_normalized
),

ranked as (
    select
        institutions.*,
        row_number() over (
            partition by institutions.group_key
            order by institutions.total_assets_usd desc, institutions.cert asc
        ) as rank_in_group
    from institutions
)

select
    ranked.cert,
    ranked.institution_name,
    ranked.institution_name_normalized,
    ranked.holding_company_name_normalized,
    ranked.charter_class,
    ranked.as_of_date,
    group_totals.group_assets_usd,
    -- A bank name shared by several institutions cannot identify one of them.
    name_counts.institutions_with_name = 1 as is_unique_institution_name,
    ranked.rank_in_group = 1 as is_group_lead
from ranked
inner join group_totals
    on ranked.group_key = group_totals.group_key
inner join name_counts
    on ranked.institution_name_normalized = name_counts.institution_name_normalized
