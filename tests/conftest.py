import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    return {
        'database': 'spark_catalog',
        'schema': 'default',
        'type': 'iomete',
        'host': 'dev.iomete.cloud',
        'domain': os.getenv("DBT_IOMETE_DOMAIN"),
        'lakehouse': os.getenv("DBT_IOMETE_LAKEHOUSE"),
        'user': os.getenv("DBT_IOMETE_USER_NAME"),
        'token': os.getenv("DBT_IOMETE_TOKEN"),
        'port': int(os.getenv("DBT_IOMETE_PORT")),
        'dataplane': os.getenv("DBT_IOMETE_DATAPLANE"),
    }


generate_schema_name_macro = """
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
"""

@pytest.fixture(scope="class")
def macros():
    return {
        "generate_schema_name_macro.sql": generate_schema_name_macro
    }