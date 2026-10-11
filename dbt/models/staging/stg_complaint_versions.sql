-- Every version of every complaint, including versions of complaints later removed.
select
    {{ safe_int('complaint_id') }} as complaint_id,
    cast(dbt_valid_from as date) as valid_from,
    cast(dbt_valid_to as date) as valid_to,
    company as company_name,
    company_response,
    company_public_response,
    timely_response = 'Yes' as is_timely_response,
    status_hash,
    attribute_hash
from {{ ref('snap_complaints') }}
