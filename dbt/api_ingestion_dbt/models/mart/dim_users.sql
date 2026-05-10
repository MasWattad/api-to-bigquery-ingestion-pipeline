{{ config(materialized='table', schema='api_mart') }}

select
  user_id,
  email,
  username,
  first_name,
  last_name,
  city,
  street,
  phone
from {{ ref('stg_users') }}