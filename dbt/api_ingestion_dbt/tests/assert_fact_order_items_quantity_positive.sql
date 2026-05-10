select
  cart_id,
  product_id,
  quantity
from {{ ref('fact_order_items') }}
where quantity <= 0