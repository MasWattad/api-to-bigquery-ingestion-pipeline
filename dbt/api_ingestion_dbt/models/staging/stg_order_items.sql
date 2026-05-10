{{ config(materialized='view', schema='api_stg') }}

with carts as (
  select
    cast(json_value(raw_record, '$.id') as int64) as cart_id,
    cast(json_value(raw_record, '$.userId') as int64) as user_id,
    date(json_value(raw_record, '$.date')) as order_date,
    json_query_array(raw_record, '$.products') as products,

    _extracted_at_utc,
    _loaded_at_utc,
    _source_endpoint,
    _source_file,
    _pipeline_run_id

  from {{ source('api_raw', 'raw_carts') }}
),

flattened as (
  select
    cart_id,
    user_id,
    order_date,
    cast(json_value(product_item, '$.productId') as int64) as product_id,
    cast(json_value(product_item, '$.quantity') as int64) as quantity,

    _extracted_at_utc,
    _loaded_at_utc,
    _source_endpoint,
    _source_file,
    _pipeline_run_id

  from carts,
  unnest(products) as product_item
)

select *
from flattened