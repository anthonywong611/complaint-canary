-- Active signals for the most recently evaluated week: the content of the weekly digest.
with alerts as (
    select * from {{ ref('fct_scam_alerts') }}
    where alert_status = 'active'
),

latest_week as (
    select max(week_start) as week_start
    from {{ ref('int_alert_evaluations') }}
)

select
    cast(alerts.week_start as date) as week_start,
    cast(alerts.scope as string) as scope,
    cast(coalesce(companies.company_name, geographies.state_name, alerts.geography_key) as string) as entity_name,
    cast(alerts.fraud_category as string) as fraud_category,
    cast(alerts.complaint_count as int64) as complaint_count,
    cast(alerts.baseline_median as {{ float_type() }}) as baseline_median,
    cast(alerts.robust_z_score as {{ float_type() }}) as robust_z_score,
    cast(alerts.lift as {{ float_type() }}) as lift,
    cast(alerts.severity as string) as severity,
    cast(alerts.first_raised_date as date) as first_raised_date
from alerts
inner join latest_week
    on alerts.week_start = latest_week.week_start
left join {{ ref('dim_company') }} as companies
    on alerts.company_key = companies.company_key
left join {{ ref('dim_geography') }} as geographies
    on alerts.geography_key = geographies.geography_key
