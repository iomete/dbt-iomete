{% snapshot snapshot_diff_catalog_schema_check %}

{{
  config(
    target_database=var('target_database', target.database),
    target_schema=var('target_schema', target.schema),
    strategy='check',
    unique_key='id',
    check_cols=['field1']
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
