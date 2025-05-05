{% snapshot snapshot_diff_schema %}

{{
  config(
    target_schema=var('target_schema', target.schema),
    strategy='timestamp',
    unique_key='id',
    updated_at='updated_at'
  )
}}

    select * from {{ ref('base_table') }}

{% endsnapshot %}
