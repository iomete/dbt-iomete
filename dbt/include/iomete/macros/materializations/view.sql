{% materialization view, adapter='iomete' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
