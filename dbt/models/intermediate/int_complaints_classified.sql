{{ config(materialized='view') }}

-- Complaints with their fraud category. CFPB reorganised its products and issues on
-- 2017-04-24 and older complaints keep their original labels; the taxonomy seed maps
-- labels from both eras to one stable category.
with complaints as (
    select * from {{ ref('stg_complaints') }}
),

taxonomy as (
    select
        issue,
        coalesce(sub_issue, '') as sub_issue,
        fraud_category
    from {{ ref('fraud_taxonomy') }}
)

select
    complaints.*,
    {{ hash_text('complaints.company_name_normalized') }} as company_key,
    {{ hash_columns(['complaints.product', 'complaints.sub_product']) }} as product_key,
    {{ hash_columns(['complaints.issue', 'complaints.sub_issue']) }} as issue_key,
    coalesce(complaints.state_code, 'UNKNOWN') as geography_key,
    coalesce(taxonomy.fraud_category, 'not_fraud') as fraud_category,
    coalesce(taxonomy.fraud_category, 'not_fraud') <> 'not_fraud' as is_fraud_related,
    case
        when complaints.received_date < cast('{{ var("taxonomy_cutoff") }}' as date) then 'legacy'
        else 'current'
    end as taxonomy_era
from complaints
left join taxonomy
    on
        complaints.issue = taxonomy.issue
        and coalesce(complaints.sub_issue, '') = taxonomy.sub_issue
