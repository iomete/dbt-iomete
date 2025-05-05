{% snapshot snapshot_same_schema_catalog %}

{{
  config(
    target_schema=target.schema,
    strategy='timestamp',
    unique_key='id',
    updated_at='updated_at'
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
