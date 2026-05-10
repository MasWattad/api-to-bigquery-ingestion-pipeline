{{ config(materialized='table', schema='api_mart') }}

select
  order_date,
  count(distinct cart_id) as orders,
  count(*) as order_items,
  sum(quantity) as total_items_sold,
  sum(item_revenue) as total_revenue,
  safe_divide(sum(item_revenue), count(distinct cart_id)) as avg_order_value
from {{ ref('fact_order_items') }}
group by order_date