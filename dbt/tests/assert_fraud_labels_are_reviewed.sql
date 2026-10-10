{{ config(severity='error', warn_if='> 0', error_if='> 10', meta={'dagster': {'ref': {'name': 'stg_complaints'}}}) }}

-- Taxonomy drift: a label that reads like fraud but is missing from the fraud_taxonomy
-- seed. CFPB adds labels over time; each one needs a decision.
with labels as (
    select distinct
        issue,
        coalesce(sub_issue, '') as sub_issue
    from {{ ref('stg_complaints') }}
    where issue is not null
),

reviewed as (
    select
        issue,
        coalesce(sub_issue, '') as sub_issue
    from {{ ref('fraud_taxonomy') }}
)

select
    labels.issue,
    labels.sub_issue
from labels
left join reviewed
    on
        labels.issue = reviewed.issue
        and labels.sub_issue = reviewed.sub_issue
where
    reviewed.issue is null
    and {{ regex_match("lower(labels.issue || ' | ' || labels.sub_issue)", var('fraud_label_pattern')) }}
