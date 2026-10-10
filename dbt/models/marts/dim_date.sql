select
    {{ date_key('date_day') }} as date_key,
    date_day,
    week_start,
    cast(extract(year from date_day) as int64) as year,
    cast(extract(month from date_day) as int64) as month
from {{ ref('int_date_spine') }}
