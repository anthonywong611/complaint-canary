with labels as (
    select
        issue_key,
        issue,
        sub_issue,
        fraud_category,
        min(received_date) as first_received_date,
        max(received_date) as last_received_date
    from {{ ref('int_complaints_classified') }}
    group by issue_key, issue, sub_issue, fraud_category
)

select
    cast(issue_key as string) as issue_key,
    cast(issue as string) as issue,
    cast(sub_issue as string) as sub_issue,
    cast(
        case
            when last_received_date < cast('{{ var("taxonomy_cutoff") }}' as date) then 'legacy'
            when first_received_date >= cast('{{ var("taxonomy_cutoff") }}' as date) then 'current'
            else 'both'
        end as string
    ) as taxonomy_era,
    cast(fraud_category as string) as fraud_category
from labels
