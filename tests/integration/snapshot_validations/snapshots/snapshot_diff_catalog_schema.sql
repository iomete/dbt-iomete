{% snapshot snapshot_diff_catalog_schema %}

{{
  config(
    target_database=var('target_database', target.database),
    target_schema=var('target_schema', target.schema),
    strategy='timestamp',
    unique_key='id',
    updated_at='updated_at'
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
