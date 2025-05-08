{% materialization incremental, adapter='iomete', supported_languages=['sql', 'python'] -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='iceberg') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'merge' -%}
  
  {%- set file_format = dbt_iomete_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_iomete_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- for SQL model we will create temp view that doesn't have database and schema --#}
  {%- if language == 'sql'-%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {% if strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {% endcall %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, compiled_code, language) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, compiled_code, language) %}
  {% else %}
    {%- call statement('create_temp_relation', language=language) -%}
      {{ create_table_as(True, tmp_relation, compiled_code, language) }}
    {%- endcall -%}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {% set build_sql = dbt_iomete_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, incremental_predicates) %}
  {% endif %}

  {%- call statement('main', language=language) -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}
  
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
