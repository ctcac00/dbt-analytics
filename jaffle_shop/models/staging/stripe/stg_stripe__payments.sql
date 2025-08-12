select
    orderid as order_id,
    amount
from {{ source('stripe', 'payment') }}
