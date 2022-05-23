from tests.integration.base import DBTIntegrationTest


class TestStoreFailures(DBTIntegrationTest):
    @property
    def schema(self):
        return "store_failures"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'tests': {
                '+store_failures': True,
                '+severity': 'warn',
                '+file_format': 'parquet',
            }
        }

    def test_store_failures(self):
        self.run_dbt(['run'])
        self.run_dbt(['test', '--store-failures'])


class TestStoreFailuresIceberg(TestStoreFailures):

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'tests': {
                '+store_failures': True,
                '+severity': 'warn'
            }
        }

    def test_store_failures_iceberg(self):
        self.test_store_failures()
