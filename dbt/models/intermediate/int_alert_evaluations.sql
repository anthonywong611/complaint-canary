-- The alert rule, applied to every mature week of every series.
-- Median and MAD are computed by ranking so the result is exact and identical on
-- BigQuery and DuckDB. See docs/alert-rule.md for the reasoning behind each threshold.
{% set baseline_weeks = var('baseline_weeks') %}
{% set middle_ranks -%}
    cast(floor((baseline_weeks + 1) / 2) as int64), cast(floor((baseline_weeks + 2) / 2) as int64)
{%- endset %}

with counts as (
    select * from {{ ref('int_weekly_series_counts') }}
),

evaluated as (
    select * from counts
    where is_mature
),

baseline_rows as (
    select
        evaluated.series_key,
        evaluated.week_start,
        baseline.complaint_count as baseline_count,
        baseline.entity_total_count as baseline_total_count
    from evaluated
    inner join counts as baseline
        on
            evaluated.series_key = baseline.series_key
            and {{ add_days('evaluated.week_start', -7 * baseline_weeks) }} <= baseline.week_start
            and evaluated.week_start > baseline.week_start
),

ranked_counts as (
    select
        *,
        row_number() over (partition by series_key, week_start order by baseline_count) as value_rank,
        count(*) over (partition by series_key, week_start) as baseline_weeks
    from baseline_rows
),

medians as (
    select
        series_key,
        week_start,
        max(baseline_weeks) as baseline_weeks,
        avg(case when value_rank in ({{ middle_ranks }}) then baseline_count end) as baseline_median,
        sum(baseline_count) as baseline_fraud_count,
        sum(baseline_total_count) as baseline_total_count
    from ranked_counts
    group by series_key, week_start
),

ranked_deviations as (
    select
        ranked_counts.series_key,
        ranked_counts.week_start,
        ranked_counts.baseline_weeks,
        abs(ranked_counts.baseline_count - medians.baseline_median) as absolute_deviation,
        row_number() over (
            partition by ranked_counts.series_key, ranked_counts.week_start
            order by abs(ranked_counts.baseline_count - medians.baseline_median)
        ) as value_rank
    from ranked_counts
    inner join medians
        on
            ranked_counts.series_key = medians.series_key
            and ranked_counts.week_start = medians.week_start
),

mads as (
    select
        series_key,
        week_start,
        avg(case when value_rank in ({{ middle_ranks }}) then absolute_deviation end) as baseline_mad
    from ranked_deviations
    group by series_key, week_start
),

measured as (
    select
        evaluated.series_key,
        evaluated.scope,
        evaluated.entity_key,
        evaluated.fraud_category,
        evaluated.week_start,
        evaluated.complaint_count,
        evaluated.entity_total_count,
        coalesce(medians.baseline_weeks, 0) as baseline_weeks,
        medians.baseline_median,
        mads.baseline_mad,
        -- The square-root floor is the Poisson spread for a count with this median. It
        -- keeps a perfectly flat baseline (MAD of zero) from producing an infinite score.
        case
            when medians.baseline_median is not null
                then greatest(1.4826 * mads.baseline_mad, sqrt(medians.baseline_median), 1.0)
        end as baseline_scale,
        evaluated.complaint_count / nullif(evaluated.entity_total_count, 0) as fraud_share,
        medians.baseline_fraud_count / nullif(medians.baseline_total_count, 0) as baseline_fraud_share
    from evaluated
    left join medians
        on
            evaluated.series_key = medians.series_key
            and evaluated.week_start = medians.week_start
    left join mads
        on
            evaluated.series_key = mads.series_key
            and evaluated.week_start = mads.week_start
),

scored as (
    select
        *,
        (complaint_count - baseline_median) / baseline_scale as robust_z_score,
        complaint_count / nullif(baseline_median, 0) as lift,
        complaint_count - baseline_median as excess_count,
        fraud_share / nullif(baseline_fraud_share, 0) as fraud_share_lift,
        case
            when baseline_weeks < {{ baseline_weeks }} then 'insufficient_history'
            when baseline_median < {{ var('min_baseline_median') }} then 'insufficient_history'
            else 'evaluated'
        end as evaluation_status
    from measured
),

decided as (
    select
        *,
        coalesce(
            evaluation_status = 'evaluated'
            and robust_z_score >= {{ var('min_z_score') }}
            and lift >= {{ var('min_lift') }}
            and excess_count >= {{ var('min_excess') }}
            -- A company whose complaints of every kind rose together is having a
            -- bad week, not a fraud spike.
            and (scope <> 'company' or fraud_share_lift >= {{ var('min_share_lift') }}),
            false
        ) as is_alert
    from scored
)

select
    series_key,
    scope,
    entity_key,
    fraud_category,
    week_start,
    complaint_count,
    entity_total_count,
    baseline_weeks,
    baseline_median,
    baseline_mad,
    baseline_scale,
    robust_z_score,
    lift,
    excess_count,
    fraud_share_lift,
    evaluation_status,
    is_alert,
    case
        when not is_alert then 'none'
        when robust_z_score >= {{ var('critical_z_score') }} then 'critical'
        when robust_z_score >= {{ var('high_z_score') }} then 'high'
        else 'medium'
    end as severity
from decided
