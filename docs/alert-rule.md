# Alert rule

Version `v1`. Implemented in `dbt/models/intermediate/int_alert_evaluations.sql`; thresholds are dbt variables in `dbt/dbt_project.yml`.

## What an alert means

A fraud category rose sharply for one company or one state, against that series' own recent history.

Complaints are self-reported and lag the events they describe. An alert is a prompt for an analyst to look, not a finding of fraud. There are no ground-truth labels, so precision and recall cannot be measured.

## Rule

A series is one fraud category for one company or one state, counted by the week (Monday to Sunday) the CFPB received the complaint.

A week is alerted when all of these hold:

| Condition | Variable | Value |
|---|---|---|
| The week is mature: its last day is at least 15 days old | `maturity_days` | 15 |
| 12 baseline weeks exist, with a median of at least 5 complaints | `baseline_weeks`, `min_baseline_median` | 12, 5 |
| Robust z-score at least 3.5 | `min_z_score` | 3.5 |
| Count at least 1.5 times the baseline median | `min_lift` | 1.5 |
| At least 10 complaints above the baseline median | `min_excess` | 10 |
| Company scope only: the fraud share of the company's complaints is at least 1.2 times its baseline share | `min_share_lift` | 1.2 |

Robust z-score = (count − baseline median) / scale, where scale = max(1.4826 × MAD, √median, 1).

Severity: `medium` from 3.5, `high` from 5, `critical` from 8.

## Why each value

| Choice | Reason |
|---|---|
| Weekly counts | Daily counts are sparse and carry a weekday pattern. |
| 15-day maturity | CFPB publishes a complaint when the company responds or after 15 days, whichever comes first. Younger weeks are incomplete. |
| 12-week baseline | One quarter: long enough for a stable median, short enough to follow a real change in level. |
| Median and MAD | An earlier spike inside the baseline does not inflate them, as it would a mean and standard deviation. |
| 1.4826 × MAD | Makes the MAD comparable to a standard deviation for normally distributed data. |
| √median floor | The spread expected of a Poisson count. A perfectly flat baseline has a MAD of zero, which would make every increase infinite. |
| z ≥ 3.5 | The usual cut-off for modified z-scores (Iglewicz and Hoaglin, 1993). |
| Median ≥ 5 | Below this the scale estimate is too coarse to trust. |
| Lift ≥ 1.5 and excess ≥ 10 | Materiality: a statistically unusual week must also be large enough to be worth an analyst's time. |
| Share lift ≥ 1.2 | Separates a fraud-specific rise from a week in which every kind of complaint about the company rose together. |
| Severity at 5 and 8 | Tiers for triage order; not calibrated. |

## What is deliberately not fraud

`Incorrect information on your report / Information belongs to someone else` is the single largest label in the database (about 5.4 million complaints). It is dominated by bulk credit-file disputes and would swamp every other signal, so the taxonomy seed marks it reviewed and `not_fraud`. The same applies to disputes about credit-monitoring products.

CFPB has no label for account takeover. The categories are `scam`, `unauthorized_transaction`, `identity_theft` and `fraud_alert_handling`.

## Revisions

- Every run re-evaluates the trailing 8 mature weeks, because late complaints change them.
- An alert that stops meeting the rule is marked `withdrawn`, never deleted. `first_raised_date` records when this pipeline first raised it.
- `ops.publication_lag` and the `warn_publication_lag` test watch for CFPB publishing later than 15 days.

## Backtest

`marts.fct_alert_backtest` replays the rule over every week since `backtest_start` (2018-01-01).

| Data | Weeks evaluated | Series evaluated per week | Alerts | Notes |
|---|---|---|---|---|
| Synthetic fixture | 21 | 13 to 17 | 2 | The planted spike, seen for the company and for its largest state. The planted company-wide flood is not alerted. |
| CFPB, full history | Not yet run | | | Filled in after the first QA run. |

The thresholds above are reasoned defaults. They have not been tuned on the real data yet. After the first QA or PROD run, summarise the backtest with:

```sql
select
    extract(year from week_start) as year,
    count(*) as weeks,
    round(avg(series_evaluated)) as series_per_week,
    sum(alerts_raised) as alerts,
    round(avg(alerts_raised), 1) as alerts_per_week,
    round(avg(alert_rate), 4) as alert_rate,
    round(avg(repeat_rate_next_week), 2) as repeat_rate
from marts.fct_alert_backtest
group by 1
order by 1;
```

A healthy rule gives a handful of alerts a week. Many more means the thresholds are too loose; a `repeat_rate` near 1 means alerts are tracking level shifts, and the baseline should be shortened. Change a threshold by editing the variable and bumping `alert_rule_version`; old alerts keep their version.

## Limits

- No seasonality model: a category that rises every tax season will alert every tax season.
- Rates per billion dollars of assets use today's FDIC figures, not those at the time of the complaint.
- Population rates use the nearest available Census year.
