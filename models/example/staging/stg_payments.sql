select
    payment_id,
    order_id,
    payment_method,
    amount_cents
from {{ ref('raw_payments') }}
