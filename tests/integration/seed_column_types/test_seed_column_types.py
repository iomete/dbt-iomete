from tests.integration.base import DBTIntegrationTest


class TestSeedColumnTypeCast(DBTIntegrationTest):
    @property
    def schema(self):
        return "seed_column_types"
        
    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'seeds': {
                'quote_columns': False,
            },
        }

    def test_seed_column_types(self):
        self.run_dbt(["seed"])
