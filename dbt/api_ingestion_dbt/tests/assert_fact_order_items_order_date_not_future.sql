select
  cart_id,
  product_id,
  order_date
from {{ ref('fact_order_items') }}
where order_date > current_date()