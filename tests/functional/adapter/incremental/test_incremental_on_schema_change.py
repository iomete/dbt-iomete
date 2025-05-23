import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange
)
from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
    _MODELS__INCREMENTAL_IGNORE,
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
    _MODELS__INCREMENTAL_IGNORE_TARGET,
    _MODELS__INCREMENTAL_FAIL,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    _MODELS__A,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET
)
from dbt.tests.util import (
    check_relations_equal,
    run_dbt
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


# Updated model name to not conflict with merge tests
class TestIncrementalAppendOnSchemaChange:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
                "+file_format": "iceberg",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_append_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_append_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            "incremental_append_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            "incremental_append_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_append_fail.sql": _MODELS__INCREMENTAL_FAIL,
            "incremental_append_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": _MODELS__A,
            "incremental_append_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            "incremental_append_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_append_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_append_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }

    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_incremental_append_ignore(self, project):
        select = "model_a incremental_append_ignore incremental_append_ignore_target"
        compare_source = "incremental_append_ignore"
        compare_target = "incremental_append_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_append_new_columns(self, project):
        select = "model_a incremental_append_append_new_columns incremental_append_append_new_columns_target"
        compare_source = "incremental_append_append_new_columns"
        compare_target = "incremental_append_append_new_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_append_new_columns_remove_one(self, project):
        select = "model_a incremental_append_append_new_columns_remove_one incremental_append_append_new_columns_remove_one_target"
        compare_source = "incremental_append_append_new_columns_remove_one"
        compare_target = "incremental_append_append_new_columns_remove_one_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_sync_all_columns(self, project):
        select = "model_a incremental_append_sync_all_columns incremental_append_sync_all_columns_target"
        compare_source = "incremental_append_sync_all_columns"
        compare_target = "incremental_append_sync_all_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_sync_remove_only(self, project):
        select = "model_a incremental_append_sync_remove_only incremental_append_sync_remove_only_target"
        compare_source = "incremental_append_sync_remove_only"
        compare_target = "incremental_append_sync_remove_only_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_append_ignore(self, project):
        self.run_incremental_append_ignore(project)

    def test_run_incremental_append_append_new_columns(self, project):
        self.run_incremental_append_append_new_columns(project)
        self.run_incremental_append_append_new_columns_remove_one(project)

    def test_run_incremental_append_sync_all_columns(self, project):
        self.run_incremental_append_sync_all_columns(project)
        self.run_incremental_append_sync_remove_only(project)

    def test_run_incremental_append_fail_on_schema_change(self, project):
        select = "model_a incremental_append_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message
