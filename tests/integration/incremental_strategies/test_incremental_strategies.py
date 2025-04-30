import pytest
from cProfile import run
from tests.integration.base import DBTIntegrationTest
import dbt.exceptions


class TestIncrementalStrategies(DBTIntegrationTest):
    @property
    def schema(self):
        return "incremental_strategies"

    @property
    def project_config(self):
        return {
            'seeds': {
                'quote_columns': False,
            },
        }

    def seed_and_run_once(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

    def seed_and_run_twice(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["run"])


class TestDefaultAppend(TestIncrementalStrategies):
    @property
    def models(self):
        return "models"
        
    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("default_append", "expected_append")

    def test_default_append(self):
        self.run_and_test()


class TestInsertOverwrite(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_insert_overwrite"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual(
            "insert_overwrite_no_partitions", "expected_overwrite")
        self.assertTablesEqual(
            "insert_overwrite_partitions", "expected_upsert")

    @pytest.mark.skip(reason="We do not support parquet file format yet")
    def test_insert_overwrite(self):
        self.run_and_test()


class TestIcebergStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_iceberg"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("append", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")
        self.assertTablesEqual("merge_update_columns", "expected_partial_upsert")

    def test_iceberg_strategies(self):
        self.run_and_test()


class TestBadStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_bad"

    def run_and_test(self):
        results = self.run_dbt(["run"], expect_pass=False)
        # assert all models fail with compilation errors
        for result in results:
            self.assertEqual("error", result.status)
            self.assertIn("Compilation Error in model", result.message)

    def test_bad_strategies(self):
        self.run_and_test()
