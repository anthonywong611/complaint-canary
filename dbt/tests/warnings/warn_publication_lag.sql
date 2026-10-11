{{ config(severity='warn') }}

-- CFPB is publishing later than the alert rule's maturity window assumes.
select
    snapshot_date,
    newly_observed_count,
    late_share
from {{ ref('publication_lag') }}
where
    snapshot_date = {{ snapshot_date() }}
    and newly_observed_count >= {{ var('lag_min_volume') }}
    and late_share > {{ var('lag_warn_share') }}
