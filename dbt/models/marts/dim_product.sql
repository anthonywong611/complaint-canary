select distinct
    cast(product_key as string) as product_key,
    cast(product as string) as product,
    cast(sub_product as string) as sub_product
from {{ ref('int_complaints_classified') }}
