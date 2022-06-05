{% macro iomete__get_merge_sql(target, source, unique_key, dest_columns, predicates=none) %}
  {# skip dest_columns, use merge_update_columns config if provided, otherwise use "*" #}
  {%- set update_columns = config.get("merge_update_columns") -%}
  
  {% set merge_condition %}
    {% if unique_key %}
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    {% else %}
        on false
    {% endif %}
  {% endset %}
  
    merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE
      
      {{ merge_condition }}
      
      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}
    
      when not matched then insert *
{% endmacro %}