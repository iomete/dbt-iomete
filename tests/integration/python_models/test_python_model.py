import pytest
from tests.integration.base import DBTIntegrationTest


class TestPythonModels(DBTIntegrationTest):
    @property
    def schema(self):
        return "python_models"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'tests': {
                '+python_models': True,
                '+severity': 'warn',
            }
        }

    @pytest.mark.skip(reason="We do not support python models yet")
    def test_python_model(self):
        self.run_dbt(['run'])
        self.run_dbt(['test', '--python-models'])
