{{
    config(
        materialized='incremental',
        unique_key='alert_key',
        on_schema_change='append_new_columns',
    )
}}

-- Weekly scam signals. Every run re-evaluates the trailing mature weeks, because late
-- complaints revise them. An alert that no longer holds is marked withdrawn, never deleted.
{% set window_days = 7 * var('reevaluation_weeks') + var('maturity_days') + 7 %}

with evaluations as (
    select * from {{ ref('int_alert_evaluations') }}
    {% if is_incremental() %}
        where week_start >= {{ add_days(snapshot_date(), -window_days) }}
    {% endif %}
),

{% if is_incremental() %}
    existing as (
        select
            alert_key,
            first_raised_date
        from {{ this }}
    ),
{% endif %}

companies as (
    select
        company_key,
        total_assets_usd
    from {{ ref('dim_company') }}
),

population_bounds as (
    select
        state_fips,
        min(population_year) as first_year,
        max(population_year) as last_year
    from {{ ref('stg_census_state_population') }}
    group by state_fips
),

-- Population for the year of the alert; years outside the estimates use the nearest one.
state_population as (
    select
        state_codes.state_code,
        population.population_year,
        population.population,
        population_bounds.first_year,
        population_bounds.last_year
    from {{ ref('stg_census_state_population') }} as population
    inner join {{ ref('state_codes') }} as state_codes
        on population.state_fips = state_codes.state_fips
    inner join population_bounds
        on population.state_fips = population_bounds.state_fips
),

keyed as (
    select
        evaluations.*,
        {{ hash_columns(['evaluations.series_key', 'evaluations.week_start', "'" ~ var('alert_rule_version') ~ "'"]) }}
            as alert_key,
        case when evaluations.scope = 'company' then evaluations.entity_key end as company_key,
        case when evaluations.scope = 'state' then evaluations.entity_key end as geography_key,
        cast(extract(year from evaluations.week_start) as int64) as week_year
    from evaluations
)

select
    cast(keyed.alert_key as string) as alert_key,
    cast(keyed.week_start as date) as week_start,
    {{ date_key('keyed.week_start') }} as week_start_date_key,
    cast(keyed.scope as string) as scope,
    cast(keyed.company_key as string) as company_key,
    cast(keyed.geography_key as string) as geography_key,
    cast(keyed.fraud_category as string) as fraud_category,
    cast(keyed.complaint_count as int64) as complaint_count,
    cast(keyed.baseline_median as {{ float_type() }}) as baseline_median,
    cast(keyed.baseline_scale as {{ float_type() }}) as baseline_scale,
    cast(keyed.robust_z_score as {{ float_type() }}) as robust_z_score,
    cast(keyed.lift as {{ float_type() }}) as lift,
    cast(keyed.fraud_share_lift as {{ float_type() }}) as fraud_share_lift,
    cast(keyed.complaint_count / nullif(companies.total_assets_usd / 1e9, 0) as {{ float_type() }})
        as rate_per_billion_assets,
    cast(keyed.complaint_count / nullif(state_population.population / 1e5, 0) as {{ float_type() }})
        as rate_per_100k_population,
    cast(keyed.severity as string) as severity,
    cast(case when keyed.is_alert then 'active' else 'withdrawn' end as string) as alert_status,
    {% if is_incremental() %}
        cast(coalesce(existing.first_raised_date, {{ snapshot_date() }}) as date) as first_raised_date,
    {% else %}
        cast({{ snapshot_date() }} as date) as first_raised_date,
    {% endif %}
    cast({{ snapshot_date() }} as date) as last_evaluated_date,
    cast('{{ var("alert_rule_version") }}' as string) as alert_rule_version
from keyed
left join companies
    on keyed.company_key = companies.company_key
left join state_population
    on
        keyed.geography_key = state_population.state_code
        and state_population.population_year
        = least(greatest(keyed.week_year, state_population.first_year), state_population.last_year)
{% if is_incremental() %}
    left join existing
        on keyed.alert_key = existing.alert_key
    where keyed.is_alert or existing.alert_key is not null
{% else %}
    where keyed.is_alert
{% endif %}
