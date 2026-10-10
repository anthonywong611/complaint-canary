select
    cast(company_key as string) as company_key,
    cast(company_name as string) as company_name,
    cast(company_name_normalized as string) as company_name_normalized,
    cast(fdic_cert as int64) as fdic_cert,
    cast(fdic_name as string) as fdic_name,
    cast(match_method as string) as match_method,
    cast(entity_type as string) as entity_type,
    cast(
        case
            when total_assets_usd is null then null
            when total_assets_usd < 10e9 then 'under_10b'
            when total_assets_usd < 100e9 then '10b_to_100b'
            when total_assets_usd < 1000e9 then '100b_to_1t'
            else 'over_1t'
        end as string
    ) as asset_size_band,
    cast(charter_class as string) as charter_class,
    cast(total_assets_usd as {{ float_type() }}) as total_assets_usd,
    cast(entity_type = 'bank' as bool) as is_bank,
    cast(fdic_as_of_date as date) as fdic_as_of_date
from {{ ref('int_company_resolved') }}
