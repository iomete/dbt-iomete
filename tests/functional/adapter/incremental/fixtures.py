seeds__duplicate_insert_sql = """
-- Insert statement which when applied to seed.csv triggers the inplace
--   overwrite strategy of incremental models. Seed and incremental model
--   diverge.

-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford',DATE '2022-02-14');

"""

seeds__seed_csv = """state,county,city,last_visit_date
CT,Hartford,Hartford,2020-09-23
MA,Suffolk,Boston,2020-02-12
NJ,Mercer,Trenton,2022-01-01
NY,Kings,Brooklyn,2021-04-02
NY,New York,Manhattan,2021-04-01
PA,Philadelphia,Philadelphia,2021-05-21
"""

seeds__add_new_rows_sql = """
-- Insert statement which when applied to seed.csv sees incremental model
--   grow in size while not (necessarily) diverging from the seed itself.

-- insert two new rows, both of which should be in incremental model
--   with any unique columns
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle',DATE '2022-02-01');

insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CA','Los Angeles','Los Angeles',DATE '2022-02-01');

"""

models__merge_update_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_update_columns = ['msg'],
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- msg will be updated, color will be ignored
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

models__tblproperties_a = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        tblproperties:
            a: b
            c: d
    columns:
        - name: id
        - name: msg
        - name: color
"""

models__tblproperties_b = """
version: 2

models:
  - name: merge_update_columns_sql
    config:
        tblproperties:
            c: e
            d: f
    columns:
        - name: id
        - name: msg
        - name: color
"""
