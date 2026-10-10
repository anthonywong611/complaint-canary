-- How the alert rule behaves when replayed over history. There are no ground-truth
-- fraud labels, so this measures how noisy and how persistent the signals are, not
-- how accurate.
with evaluations as (
    select * from {{ ref('int_alert_evaluations') }}
),

with_next as (
    select
        evaluations.*,
        next_week.is_alert as next_week_is_alert
    from evaluations
    left join evaluations as next_week
        on
            evaluations.series_key = next_week.series_key
            and next_week.week_start = {{ add_days('evaluations.week_start', 7) }}
)

select
    cast(week_start as date) as week_start,
    {{ date_key('week_start') }} as week_start_date_key,
    cast('{{ var("alert_rule_version") }}' as string) as alert_rule_version,
    cast(sum(case when evaluation_status = 'evaluated' then 1 else 0 end) as int64) as series_evaluated,
    cast(sum(case when is_alert then 1 else 0 end) as int64) as alerts_raised,
    cast(
        sum(case when is_alert then 1 else 0 end)
        / nullif(sum(case when evaluation_status = 'evaluated' then 1 else 0 end), 0)
        as {{ float_type() }}
    ) as alert_rate,
    cast(
        sum(case when is_alert and next_week_is_alert then 1 else 0 end)
        / nullif(sum(case when is_alert and next_week_is_alert is not null then 1 else 0 end), 0)
        as {{ float_type() }}
    ) as repeat_rate_next_week
from with_next
group by week_start
