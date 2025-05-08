import pytest

from dbt.tests import util
from tests.functional.adapter.tblproperties import fixtures


def _check_tblproperties(project, model_name: str, properties: list[str]):
    results = util.run_sql_with_adapter(
        project.adapter,
        f"show tblproperties {project.test_schema}.{model_name}",
        fetch="all",
    )
    tblproperties = [result[0] for result in results]

    for prop in properties:
        assert prop in tblproperties


class TestTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": fixtures.table_sql,
            "set_tblproperties.sql": fixtures.table_tblproperties_sql,
            "set_tblproperties_to_view.sql": fixtures.view_tblproperties_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "set_tblproperties_to_snapshot.sql": fixtures.snapshot_tblproperties_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": fixtures.seed_csv,
            "set_tblproperties_to_seed.csv": fixtures.seed_tblproperties_csv,
        }

    def test_set_tblproperties_to_table(self, project):
        util.run_dbt(["seed", "-s", "expected"])
        util.run_dbt(["run", "-s", "set_tblproperties"])
        util.run_dbt(["run", "-s", "set_tblproperties"])

        util.check_relations_equal(project.adapter, ["set_tblproperties", "expected"])
        _check_tblproperties(project, "set_tblproperties", ["tblproperties_to_table"])

    def test_set_tblproperties_to_view(self, project):
        util.run_dbt(["seed", "-s", "expected"])
        util.run_dbt(["run", "-s", "table"])
        util.run_dbt(["run", "-s", "set_tblproperties_to_view"])

        util.check_relations_equal(project.adapter, ["set_tblproperties_to_view", "expected"])
        _check_tblproperties(project, "set_tblproperties_to_view", ["tblproperties_to_view"])

    def test_set_tblproperties_to_snapshot(self, project):
        util.run_dbt(["seed", "-s", "expected"])
        util.run_dbt(["run", "-s", "table"])
        util.run_dbt(["snapshot", "-s", "set_tblproperties_to_snapshot"])

        util.check_relations_equal(project.adapter, ["set_tblproperties_to_snapshot", "expected"])
        _check_tblproperties(project, "set_tblproperties_to_snapshot", ["tblproperties_to_snapshot"])

    def test_set_tblproperties_to_seed(self, project):
        util.run_dbt(["seed", "-s", "expected"])
        util.write_file(fixtures.seed_tblproperties, "seeds", "schema.yml")
        util.run_dbt(["seed", "-s", "set_tblproperties_to_seed"])

        util.check_relations_equal(project.adapter, ["set_tblproperties_to_seed", "expected"])
        _check_tblproperties(project, "set_tblproperties_to_seed", ["tblproperties_to_seed"])