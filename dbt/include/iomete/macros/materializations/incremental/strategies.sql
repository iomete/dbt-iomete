{% macro get_insert_overwrite_sql(source_relation, target_relation) %}
    
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute='quoted') | list -%}
    {%- set source_columns = adapter.get_columns_in_relation(source_relation) | map(attribute='quoted') | list -%}

    insert into table {{ target_relation }} ({{ dest_columns | join(', ') }})
    select
        {%- for col in dest_columns %}
            {%- if col in source_columns -%}
                {{ col }}
            {%- else -%}
                NULL AS {{ col }}
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
        {%- endfor %}
    from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro iomete__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set source_columns = adapter.get_columns_in_relation(source) | map(attribute='quoted') | list -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set all_update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {%- set update_columns = [] -%}
  {%- for column_name in all_update_columns -%}
    {% if column_name in source_columns %}
      {% do update_columns.append(column_name) %}
    {% endif %}
  {%- endfor -%}

  {%- set insert_columns = [] -%}
  {%- for col in dest_columns -%}
    {% if col.quoted in source_columns %}
      {% do insert_columns.append(col.quoted) %}
    {% endif %}
  {%- endfor -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert
        {% if insert_columns -%}
          ({{ insert_columns | join(', ') }}) values (
            {%- for column_name in insert_columns %}
              DBT_INTERNAL_SOURCE.{{ column_name }}{% if not loop.last %}, {% endif %}
            {%- endfor %}
          )
        {%- else %} * {% endif %}
{% endmacro %}


{% macro dbt_iomete_get_incremental_sql(strategy, source, target, unique_key, incremental_predicates) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with iceberg table - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, incremental_predicates=incremental_predicates) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
