{% materialization table, adapter = 'iomete', supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {% set database = model.database %}
  {% set schema = model.schema %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is iceberg, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {%- set raw_file_format = config.get('file_format', default='iceberg') -%}
  {% set is_iceberg_file_format = raw_file_format == 'iceberg' %}

  {% if old_relation and not (old_relation.is_iceberg and is_iceberg_file_format) -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model
  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}
  
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}


{% macro py_write_table(compiled_code, target_relation) %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('dbtModels').getOrCreate()

{{ compiled_code }}

dbt = dbtObj(spark.read.format("iceberg").load)
df = model(dbt, spark)

import pyspark

df.write.mode("overwrite").format("iceberg").option("overwriteSchema", "true").saveAsTable("{{ target_relation }}")
{%- endmacro -%}