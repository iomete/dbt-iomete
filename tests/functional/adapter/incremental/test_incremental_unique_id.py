import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey
from tests.functional.adapter.incremental import fixtures


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "iceberg",
                "+incremental_strategy": "merge",
            }
        }

    # Override seeds fixture to handle Iceberg's strict type conversion (String to DATE not allowed)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": fixtures.seeds__duplicate_insert_sql,
            "seed.csv": fixtures.seeds__seed_csv,
            "add_new_rows.sql": fixtures.seeds__add_new_rows_sql,
        }
