select
    cert,
    institution_name,
    {{ normalize_company_name('institution_name') }} as institution_name_normalized,
    holding_company_name,
    {{ normalize_company_name('holding_company_name') }} as holding_company_name_normalized,
    state_code,
    charter_class,
    cast(total_assets_thousands as {{ float_type() }}) * 1000 as total_assets_usd,
    as_of_date
from {{ source('raw', 'fdic_institutions') }}
