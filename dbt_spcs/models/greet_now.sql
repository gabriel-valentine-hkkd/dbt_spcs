-- models/greet_now.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'run_date',
    on_schema_change = 'sync_all_columns'
) }}

select
  current_date::date  as run_date,
  current_timestamp() as run_ts,
  'hola desde el modelo greet_now' as message

{% if is_incremental() %}
  where run_date > (select coalesce(max(run_date),'1900-01-01') from {{ this }})
{% endif %}
