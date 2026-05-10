{{ config(materialized='view', schema='api_stg') }}

select
  cast(json_value(raw_record, '$.id') as int64) as user_id,
  json_value(raw_record, '$.email') as email,
  json_value(raw_record, '$.username') as username,
  json_value(raw_record, '$.name.firstname') as first_name,
  json_value(raw_record, '$.name.lastname') as last_name,
  json_value(raw_record, '$.address.city') as city,
  json_value(raw_record, '$.address.street') as street,
  json_value(raw_record, '$.phone') as phone,

  _extracted_at_utc,
  _loaded_at_utc,
  _source_endpoint,
  _source_file,
  _pipeline_run_id

from {{ source('api_raw', 'raw_users') }}