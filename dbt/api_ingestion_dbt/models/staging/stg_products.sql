{{ config(materialized='view', schema='api_stg') }}

select
  cast(json_value(raw_record, '$.id') as int64) as product_id,
  json_value(raw_record, '$.title') as product_title,

  lower(
    replace(json_value(raw_record, '$.category'), "'", "")
  ) as category,

  cast(json_value(raw_record, '$.price') as numeric) as price,
  cast(json_value(raw_record, '$.rating.rate') as numeric) as rating_rate,
  cast(json_value(raw_record, '$.rating.count') as int64) as rating_count,

  _extracted_at_utc,
  _loaded_at_utc,
  _source_endpoint,
  _source_file,
  _pipeline_run_id

from {{ source('api_raw', 'raw_products') }}