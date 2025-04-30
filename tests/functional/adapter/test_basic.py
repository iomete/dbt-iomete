import os
import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests

pytest_plugins = ["dbt.tests.fixtures.project"]

schema_base_yml = """
version: 2
sources:
  - name: "raw"
    database: "{{ var('database') }}"
    schema: "{{ var('schema') }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

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

class BaseTestFixtures:
    @pytest.fixture(scope="class", name="project_config_update")
    def set_project_config_update(self, project_config_update, dbt_profile_target):
        project_config_update.update({
                "vars": {
                    "database": dbt_profile_target["database"],
                    "schema": dbt_profile_target["schema"],
                },
                "seeds": {
                    "database": dbt_profile_target["database"],
                    "schema": dbt_profile_target["schema"],
                },
                "models": {
                    "database": dbt_profile_target["database"],
                    "schema": dbt_profile_target["schema"],
                },
                "snapshots": {
                    "database": dbt_profile_target["database"],
                    "schema": dbt_profile_target["schema"],
                }
            })
        return project_config_update

    @pytest.fixture(scope="class", name="models")
    def set_models(self, models):
        models.update({"schema.yml": schema_base_yml})
        return models

class TestSimpleMaterializationsIomete(BaseTestFixtures, BaseSimpleMaterializations):
    pass

class TestSingularTestsIomete(BaseSingularTests):
    pass

class TestSingularTestsEphemeralIomete(BaseTestFixtures, BaseSingularTestsEphemeral):
    pass

class TestEmptyIomete(BaseEmpty):
    pass

class TestEphemeralIomete(BaseTestFixtures, BaseEphemeral):
    pass

class TestIncrementalIomete(BaseTestFixtures, BaseIncremental):
    pass

class TestGenericTestsIomete(BaseTestFixtures, BaseGenericTests):
    pass

class TestSnapshotCheckColsIomete(BaseSnapshotCheckCols):
    pass

class TestSnapshotTimestampIomete(BaseSnapshotTimestamp):
    pass

class TestBaseAdapterMethodIomete(BaseAdapterMethod):
    pass