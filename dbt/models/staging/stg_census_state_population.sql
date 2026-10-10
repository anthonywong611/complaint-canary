select
    state_fips,
    state_name,
    cast(population_year as int64) as population_year,
    cast(population as int64) as population
from {{ source('raw', 'census_state_population') }}
