from tests.integration.base import DBTIntegrationTest

ALT_SCHEMA = 'dbt_experiment'
ALT_CATALOG = 'test_dbt_multi_catalog'
ALT_CATALOG_SCHEMA = 'test_schema'

class TestSnapshotStrategies(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_snapshots"

    @property
    def models(self):
        return "models"

    @property
    def snapshots(self):
        return "snapshots"

    @property
    def seeds(self):
        return "seeds"

    def run_snapshot_versions_and_columns(self, snapshot_name, full_snapshot_path, snapshot_vars):
        print(f"Running snapshot test for: {snapshot_name}")
        self.run_sql(f"DROP TABLE IF EXISTS {full_snapshot_path}")
        self.run_dbt(["seed"])
        self.run_dbt(["run", "-s", "base_table"])
        snapshot_cmd = ["snapshot", "-s", snapshot_name]
        if snapshot_vars:
            snapshot_cmd += ["--vars", snapshot_vars]
        self.run_dbt(snapshot_cmd)

        # Modify data
        self.run_sql(f"""UPDATE {{schema}}.base_table SET field1='UPDATED', updated_at=CAST('2023-01-03 00:00:00' AS TIMESTAMP) WHERE id = 1;""")
        self.run_dbt(snapshot_cmd)

        # Validate row versioning
        results = self.run_sql(f"SELECT COUNT(*) FROM {full_snapshot_path} WHERE id = 1 LIMIT 1", fetch='one')
        assert results[0] == 2, f"Expected 2 versions for id=1 in {snapshot_name}, found {results[0]}"

        # Validate snapshots columns
        col_results = self.run_sql(f"DESC {full_snapshot_path}", fetch='all')
        actual_cols = {row[0] for row in col_results}
        expected_cols = {
            "id", "field1", "field2", "field3", "field4", "updated_at",
            "dbt_scd_id", "dbt_valid_from", "dbt_valid_to"
        }
        missing = expected_cols - actual_cols
        assert not missing, f"Missing columns in {snapshot_name}: {missing}"

    def test_snapshot_same_schema_catalog(self):
        snapshot_name = 'snapshot_same_schema_catalog'
        full_snapshot_path = f"""{{database}}.{{schema}}.{snapshot_name}"""
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, None)

    def test_snapshot_same_schema_catalog_check(self):
        snapshot_name = 'snapshot_same_schema_catalog_check'
        full_snapshot_path = f"""{{database}}.{{schema}}.{snapshot_name}"""
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, None)

    def test_snapshot_diff_schema(self):
        snapshot_name = 'snapshot_diff_schema'
        full_snapshot_path = f"""{{database}}.{ALT_SCHEMA}.{snapshot_name}"""
        snapshot_vars = f'{{"target_schema": "{ALT_SCHEMA}"}}'
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, snapshot_vars)

    def test_snapshot_diff_schema_check(self):
        snapshot_name = 'snapshot_diff_schema_check'
        full_snapshot_path = f"""{{database}}.{ALT_SCHEMA}.{snapshot_name}"""
        snapshot_vars = f'{{"target_schema": "{ALT_SCHEMA}"}}'
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, snapshot_vars)

    def test_snapshot_diff_catalog_schema(self):
        snapshot_name = 'snapshot_diff_catalog_schema'
        full_snapshot_path = f"""{ALT_CATALOG}.{ALT_CATALOG_SCHEMA}.{snapshot_name}"""
        snapshot_vars = f'{{"target_database": "{ALT_CATALOG}", "target_schema": "{ALT_CATALOG_SCHEMA}"}}'
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, snapshot_vars)

    def test_snapshot_diff_catalog_schema_check(self):
        snapshot_name = 'snapshot_diff_catalog_schema_check'
        full_snapshot_path = f"""{ALT_CATALOG}.{ALT_CATALOG_SCHEMA}.{snapshot_name}"""
        snapshot_vars = f'{{"target_database": "{ALT_CATALOG}", "target_schema": "{ALT_CATALOG_SCHEMA}"}}'
        self.run_snapshot_versions_and_columns(snapshot_name, full_snapshot_path, snapshot_vars)