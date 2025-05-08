table_sql = """
{{ config(
    materialized = 'table',
)}}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg
"""

table_tblproperties_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    tblproperties={
      'tblproperties_to_table' : 'true'
    }
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
"""

view_tblproperties_sql = """
{{ config(
    tblproperties={
      'tblproperties_to_view' : 'true'
    }
) }}

select * from {{ ref('table') }}
"""

snapshot_tblproperties_sql = """
{% snapshot set_tblproperties_to_snapshot %}

    {{
        config(
          check_cols=["msg"],
          unique_key="id",
          strategy="check",
          target_schema=schema,
          tblproperties={
            'tblproperties_to_snapshot' : 'true'
          }
        )
    }}

    select * from {{ ref('table') }}

{% endsnapshot %}
"""

seed_tblproperties_csv = """id,msg
1,hello
2,yo
3,anyway
"""

seed_tblproperties = """
version: 2

seeds:
  - name: set_tblproperties_to_seed
    config:
        tblproperties:
            tblproperties_to_seed: true
    columns:
        - name: id
        - name: msg
"""

seed_csv = """id,msg
1,hello
2,yo
3,anyway
"""