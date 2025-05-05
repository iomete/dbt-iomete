import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target(request):
    return {
        'database': 'spark_catalog',
        'schema': 'default',
        'type': 'iomete',
        'host': 'dev.iomete.cloud',
        'domain': os.getenv("DBT_IOMETE_DOMAIN"),
        'lakehouse': os.getenv("DBT_IOMETE_LAKEHOUSE"),
        'user': os.getenv("DBT_IOMETE_USER_NAME"),
        'token': os.getenv("DBT_IOMETE_TOKEN"),
        'port': int(os.getenv("DBT_IOMETE_PORT")),
        'dataplane': os.getenv("DBT_IOMETE_DATAPLANE"),
        'threads': 10
    }
