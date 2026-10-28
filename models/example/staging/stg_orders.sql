select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    status as order_status
from {{ ref('raw_orders') }}
