select
  cart_id,
  product_id,
  item_revenue
from {{ ref('fact_order_items') }}
where item_revenue < 0