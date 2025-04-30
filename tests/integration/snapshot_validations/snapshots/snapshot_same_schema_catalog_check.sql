{% snapshot snapshot_same_schema_catalog_check %}

{{
  config(
    target_schema=target.schema,
    strategy='check',
    unique_key='id',
    check_cols=['field1']
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
