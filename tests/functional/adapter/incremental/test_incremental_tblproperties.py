import pytest

from dbt.tests import util
from tests.functional.adapter.incremental import fixtures

@pytest.mark.skip(reason="TODO: Add support to update tblproperties")
class TestIncrementalTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.models__merge_update_columns_sql,
            "schema.yml": fixtures.models__tblproperties_a,
        }

    def test_changing_tblproperties(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.models__tblproperties_b, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "show tblproperties {database}.{schema}.merge_update_columns_sql",
            fetch="all",
        )
        results_dict = {}

        for key, value in results:
            results_dict[key] = value

        assert results_dict["c"] == "e"
        assert results_dict["d"] == "f"
