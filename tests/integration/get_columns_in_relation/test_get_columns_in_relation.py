from tests.integration.base import DBTIntegrationTest


class TestGetColumnInRelationInSameRun(DBTIntegrationTest):
    @property
    def schema(self):
        return "get_columns_in_relation"

    @property
    def models(self):
        return "models"

    def run_and_test(self):
        self.run_dbt(["run"])
        self.assertTablesEqual("child", "get_columns_from_child")

    def test_get_columns_in_relation_in_same_run(self):
        self.run_and_test()
