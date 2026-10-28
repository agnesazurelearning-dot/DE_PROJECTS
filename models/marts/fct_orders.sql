with orders as (
    select * from {{ ref('stg_orders') }}
),
 
payments as (
    select
        order_id,
        payment_method,
        {{ cents_to_dollars('amount_cents') }} as amount_dollars
    from {{ ref('stg_payments') }}
)
 
select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.order_status,
    payments.payment_method,
    payments.amount_dollars
from orders
left join payments
    on orders.order_id = payments.order_id
