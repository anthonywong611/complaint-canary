-- Weekly fraud-complaint counts per series, with zero-filled weeks.
-- A series is a fraud category for one company or one state. Only series that could
-- ever meet the alert rule's volume floor are kept, which bounds the grid.
{% set half_baseline = (var('baseline_weeks') / 2) | round(0, 'ceil') | int %}

with complaints as (
    select
        {{ week_start('received_date') }} as week_start,
        company_key,
        geography_key,
        fraud_category,
        is_fraud_related
    from {{ ref('int_complaints_classified') }}
    where received_date >= cast('{{ var("backtest_start") }}' as date)
),

weeks as (
    select distinct week_start
    from {{ ref('int_date_spine') }}
    where
        week_start >= {{ week_start("cast('" ~ var('backtest_start') ~ "' as date)") }}
        and week_start <= {{ week_start(snapshot_date()) }}
),

entity_weeks as (
    select
        'company' as scope,
        company_key as entity_key,
        week_start,
        fraud_category,
        is_fraud_related
    from complaints
    union all
    select
        'state' as scope,
        geography_key as entity_key,
        week_start,
        fraud_category,
        is_fraud_related
    from complaints
    where geography_key <> 'UNKNOWN'
),

fraud_counts as (
    select
        scope,
        entity_key,
        fraud_category,
        week_start,
        count(*) as complaint_count
    from entity_weeks
    where is_fraud_related
    group by scope, entity_key, fraud_category, week_start
),

entity_totals as (
    select
        scope,
        entity_key,
        week_start,
        count(*) as entity_total_count
    from entity_weeks
    group by scope, entity_key, week_start
),

-- A baseline median of N needs at least half the baseline weeks at N or more.
series as (
    select
        scope,
        entity_key,
        fraud_category
    from fraud_counts
    group by scope, entity_key, fraud_category
    having sum(case when complaint_count >= {{ var('min_baseline_median') }} then 1 else 0 end) >= {{ half_baseline }}
),

grid as (
    select
        series.scope,
        series.entity_key,
        series.fraud_category,
        weeks.week_start
    from series
    cross join weeks
)

select
    {{ hash_columns(['grid.scope', 'grid.entity_key', 'grid.fraud_category']) }} as series_key,
    grid.scope,
    grid.entity_key,
    grid.fraud_category,
    grid.week_start,
    coalesce(fraud_counts.complaint_count, 0) as complaint_count,
    coalesce(entity_totals.entity_total_count, 0) as entity_total_count,
    -- CFPB publishes a complaint up to 15 days after receiving it, so a week is only
    -- complete once its last day is that old.
    {{ add_days('grid.week_start', 6) }} <= {{ add_days(snapshot_date(), -var('maturity_days')) }} as is_mature
from grid
left join fraud_counts
    on
        grid.scope = fraud_counts.scope
        and grid.entity_key = fraud_counts.entity_key
        and grid.fraud_category = fraud_counts.fraud_category
        and grid.week_start = fraud_counts.week_start
left join entity_totals
    on
        grid.scope = entity_totals.scope
        and grid.entity_key = entity_totals.entity_key
        and grid.week_start = entity_totals.week_start
