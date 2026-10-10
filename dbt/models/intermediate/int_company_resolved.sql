-- One row per complaint company, resolved to an FDIC institution where possible.
-- Order of precedence: curated override seed, exact bank name, exact holding-company name.
with spellings as (
    select
        company_name_normalized,
        company_name,
        count(*) as complaint_count
    from {{ ref('stg_complaints') }}
    where company_name_normalized is not null
    group by company_name_normalized, company_name
),

ranked_spellings as (
    select
        *,
        row_number() over (
            partition by company_name_normalized
            order by complaint_count desc, company_name asc
        ) as spelling_rank
    from spellings
),

companies as (
    select
        company_name_normalized,
        company_name
    from ranked_spellings
    where spelling_rank = 1
),

overrides as (
    select
        {{ normalize_company_name('company_name') }} as company_name_normalized,
        entity_type,
        cast(fdic_cert as int64) as fdic_cert
    from {{ ref('company_overrides') }}
),

fdic as (
    select * from {{ ref('int_fdic_entities') }}
),

by_institution as (
    select * from fdic
    where is_unique_institution_name
),

by_holding as (
    select * from fdic
    where is_group_lead and holding_company_name_normalized is not null
),

joined as (
    select
        companies.company_name_normalized,
        companies.company_name,
        overrides.company_name_normalized is not null as has_override,
        overrides.entity_type as override_entity_type,
        by_cert.cert as override_cert,
        by_institution.cert as institution_cert,
        by_holding.cert as holding_cert,
        coalesce(by_cert.institution_name, by_institution.institution_name, by_holding.institution_name)
            as matched_fdic_name,
        coalesce(by_cert.charter_class, by_institution.charter_class, by_holding.charter_class)
            as matched_charter_class,
        coalesce(by_cert.group_assets_usd, by_institution.group_assets_usd, by_holding.group_assets_usd)
            as matched_assets_usd,
        coalesce(by_cert.as_of_date, by_institution.as_of_date, by_holding.as_of_date) as matched_as_of_date
    from companies
    left join overrides
        on companies.company_name_normalized = overrides.company_name_normalized
    left join fdic as by_cert
        on overrides.fdic_cert = by_cert.cert
    left join by_institution
        on companies.company_name_normalized = by_institution.institution_name_normalized
    left join by_holding
        on companies.company_name_normalized = by_holding.holding_company_name_normalized
)

select
    {{ hash_text('company_name_normalized') }} as company_key,
    company_name,
    company_name_normalized,
    case
        when has_override then override_cert
        else coalesce(institution_cert, holding_cert)
    end as fdic_cert,
    case
        when has_override or institution_cert is not null or holding_cert is not null then matched_fdic_name
    end as fdic_name,
    case
        when has_override then 'override'
        when institution_cert is not null then 'fdic_institution'
        when holding_cert is not null then 'fdic_holding'
        else 'unmatched'
    end as match_method,
    case
        when has_override then override_entity_type
        when institution_cert is not null or holding_cert is not null then 'bank'
        else 'unreviewed'
    end as entity_type,
    matched_charter_class as charter_class,
    matched_assets_usd as total_assets_usd,
    matched_as_of_date as fdic_as_of_date
from joined
