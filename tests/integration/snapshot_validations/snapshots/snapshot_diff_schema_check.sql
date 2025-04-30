{% snapshot snapshot_diff_schema_check %}

{{
  config(
    target_schema=var('target_schema', target.schema),
    strategy='check',
    unique_key='id',
    check_cols=['field1']
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
