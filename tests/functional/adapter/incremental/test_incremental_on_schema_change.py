import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange
)

class TestIncrementalMergeOnSchemaChange(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+file_format": "iceberg",
            }
        }


class TestIncrementalAppendOnSchemaChange(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
                "+file_format": "iceberg",
            }
        }
