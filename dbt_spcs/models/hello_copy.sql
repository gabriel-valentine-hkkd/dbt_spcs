-- models/hello copy.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'run_date',                -- one row per day
    on_schema_change = 'sync_all_columns'     -- auto-add new cols
) }}

select
  current_date::date       as run_date,
  current_timestamp()      as run_ts,
  'hola desde dbt en SPCS' as message

{% if is_incremental() %}
  -- on subsequent runs, only insert rows newer than the max we already have
  where current_date::date > (select coalesce(max(run_date),'1900-01-01') from {{ this }})
{% endif %}
