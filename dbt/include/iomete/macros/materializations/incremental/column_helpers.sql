{% macro iomete__get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {%- set default_cols = dest_columns | map(attribute="quoted") | list -%}

  {%- if merge_update_columns and merge_exclude_columns -%}
    {{ exceptions.raise_compiler_error(
        'Model cannot specify merge_update_columns and merge_exclude_columns. Please update model to use only one config'
    )}}
  {%- elif merge_update_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in merge_update_columns -%}
      {%- set clean_column = column | replace('`', '') -%}
      {%- do update_columns.append('`' ~ clean_column ~ '`') -%}
    {%- endfor -%}
  {%- elif merge_exclude_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
        {%- do update_columns.append(column.quoted) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set update_columns = default_cols -%}
  {%- endif -%}

  {{ return(update_columns) }}

{% endmacro %}