import os
import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


class TestPythonModel(BasePythonModelTests):
    pass


class TestPySpark(BasePySparkTests):
    pass


class TestPythonIncrementalModel(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}
