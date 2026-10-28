select
    customer_id,
    coalesce(first_name, 'Unknown') as first_name,
    last_name,
    email
from {{ ref('raw_customers') }}
