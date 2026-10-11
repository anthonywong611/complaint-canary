-- For each snapshot, how many of the newly observed complaints were already older than
-- the maturity window. A high share means CFPB is publishing late and recent weeks are
-- less complete than the alert rule assumes.
select
    first_observed_date as snapshot_date,
    cast(count(*) as int64) as newly_observed_count,
    cast(
        sum(
            case
                when received_date < {{ add_days('first_observed_date', -var('maturity_days')) }} then 1
                else 0
            end
        ) as int64
    ) as late_count,
    cast(
        sum(
            case
                when received_date < {{ add_days('first_observed_date', -var('maturity_days')) }} then 1
                else 0
            end
        ) / count(*) as {{ float_type() }}
    ) as late_share
from {{ ref('stg_complaints') }}
where not is_backfill
group by first_observed_date
