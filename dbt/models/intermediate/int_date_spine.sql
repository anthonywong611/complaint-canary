-- One row per day from the first complaint the CFPB published (December 2011) for
-- 10,000 days, built without warehouse-specific generators.
with digits as (
    select 0 as digit
    union all
    select 1
    union all
    select 2
    union all
    select 3
    union all
    select 4
    union all
    select 5
    union all
    select 6
    union all
    select 7
    union all
    select 8
    union all
    select 9
),

numbers as (
    select ones.digit + tens.digit * 10 + hundreds.digit * 100 + thousands.digit * 1000 as day_offset
    from digits as ones
    cross join digits as tens
    cross join digits as hundreds
    cross join digits as thousands
),

days as (
    select {{ add_days("cast('2011-12-01' as date)", 'day_offset') }} as date_day
    from numbers
)

select
    date_day,
    {{ week_start('date_day') }} as week_start
from days
