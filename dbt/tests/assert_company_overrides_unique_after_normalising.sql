-- Two override rows that normalise to the same name would duplicate a company.
select
    {{ normalize_company_name('company_name') }} as company_name_normalized,
    count(*) as override_rows
from {{ ref('company_overrides') }}
group by 1
having count(*) > 1
