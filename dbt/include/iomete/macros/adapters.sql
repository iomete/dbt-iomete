{% macro iomete__options_clause() -%}
  {%- set options = config.get('options') -%}
  {%- if options is not none %}
    options (
      {%- for option in options -%}
      {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}








{% macro iomete__create_table_as(temporary, relation, sql) -%}

  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {%- set raw_file_format = config.get('file_format', default='iceberg') -%}
    {% set is_iceberg_file_format = raw_file_format == 'iceberg' %}

    {% if is_iceberg_file_format %}
      create or replace table {{ relation }}
    {% else %}
      create table {{ relation }}
    {% endif %}
    {{ file_format_clause() }}
    {{ options_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro iomete__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show tables in {{ relation }} like '*'
  {% endcall %}

  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}



{% macro list_all_relations_without_caching(schema_relation) %}
  {% call statement('list_all_relations_without_caching', fetch_result=True) -%}
    show tables in {{ schema_relation }} like '*'
  {% endcall %}

  {% do return(load_result('list_all_relations_without_caching').table) %}
{% endmacro %}

{% macro list_views_relations_without_caching(schema_relation) %}
  {% call statement('list_views_relations_without_caching', fetch_result=True) -%}
    show views in {{ schema_relation }} like '*'
  {% endcall %}

  {% do return(load_result('list_views_relations_without_caching').table) %}
{% endmacro %}

{% macro describe_table(relation) -%}
  {% call statement('describe_table', fetch_result=True) %}
      describe extended {{ relation.include(schema=(schema is not none)) }}
  {% endcall %}
  {% do return(load_result('describe_table').table) %}
{% endmacro %}



{% macro iomete__alter_column_comment(relation, column_dict) %}
  {%- set raw_file_format = config.get('file_format', default='iceberg') -%}
  {% set is_iceberg_file_format = raw_file_format == 'iceberg' %}

  {% if is_iceberg_file_format %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation }} change column 
            {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
            comment '{{ escaped_comment }}';
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

