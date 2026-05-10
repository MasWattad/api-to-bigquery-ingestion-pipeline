{{ config(materialized='table', schema='api_mart') }}

select
  oi.cart_id,
  oi.user_id,
  oi.product_id,
  oi.order_date,
  oi.quantity,
  p.price,
  oi.quantity * p.price as item_revenue,

  oi._extracted_at_utc,
  oi._loaded_at_utc,
  oi._pipeline_run_id

from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_products') }} p
  on oi.product_id = p.product_id