{% macro dbt_iomete_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}

  {% set accepted_formats = ['csv', 'json', 'jdbc', 'parquet', 'orc', 'iceberg'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}


{% macro dbt_iomete_validate_get_incremental_strategy(raw_strategy, file_format) %}
  {#-- Validate the incremental strategy #}

  {# TODO: https://linear.app/iomete/issue/PRO-236/add-parquet-csv-json-orc-text-file-format-support #}
  {% set invalid_file_format_msg -%}
    Invalid incremental file format provided: {{ file_format }}
    We only support 'iceberg' file format
  {%- endset %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'merge', 'insert_overwrite'
  {%- endset %}

  {% set invalid_merge_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You can only choose this strategy when file_format is set to 'iceberg'
  {%- endset %}
  
  {% set invalid_insert_overwrite_iceberg_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You cannot use this strategy when file_format is set to 'iceberg' (default one)
    Use the 'append' or 'merge' strategy instead
  {%- endset %}

  {% set is_iceberg_file_format = file_format is not defined or file_format == 'iceberg' %}

  {% if not is_iceberg_file_format %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% if raw_strategy not in ['append', 'merge', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if raw_strategy == 'merge' and not is_iceberg_file_format %}
      {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
    {% endif %}
    {% if raw_strategy == 'insert_overwrite' and is_iceberg_file_format %}
      {% do exceptions.raise_compiler_error(invalid_insert_overwrite_iceberg_msg) %}
    {% endif %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}
