-- One row per state code found in complaints or in the reference seed.
with observed as (
    select distinct geography_key as state_code
    from {{ ref('int_complaints_classified') }}
),

reference as (
    select
        state_code,
        state_name,
        state_fips
    from {{ ref('state_codes') }}
),

latest_population as (
    select
        state_fips,
        population,
        population_year,
        row_number() over (partition by state_fips order by population_year desc) as recency_rank
    from {{ ref('stg_census_state_population') }}
),

codes as (
    select state_code from observed
    union distinct
    select state_code from reference
)

select
    cast(codes.state_code as string) as geography_key,
    cast(codes.state_code as string) as state_code,
    cast(reference.state_name as string) as state_name,
    cast(latest_population.population as int64) as population,
    cast(latest_population.population_year as int64) as population_year
from codes
left join reference
    on codes.state_code = reference.state_code
left join latest_population
    on
        reference.state_fips = latest_population.state_fips
        and latest_population.recency_rank = 1
