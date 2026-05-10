{{ config(materialized='table', schema='api_mart') }}

select
  product_id,
  product_title,
  category,
  price,
  rating_rate,
  rating_count
from {{ ref('stg_products') }}