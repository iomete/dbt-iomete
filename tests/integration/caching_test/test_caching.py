from tests.integration.base import DBTIntegrationTest
from dbt.adapters.factory import FACTORY


class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "caching"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'quoting': {
                'identifier': False,
                'schema': False,
            }
        }

    def run_and_get_adapter(self):
        # we want to inspect the adapter that dbt used for the run, which is
        # not self.adapter. You can't do this until after you've run dbt once.
        self.run_dbt(['run'])
        return FACTORY.adapters[self.adapter_type]

    def cache_run(self):
        adapter = self.run_and_get_adapter()
        self.assertEqual(len(adapter.cache.relations), 1)
        relation = next(iter(adapter.cache.relations.values()))
        self.assertEqual(relation.inner.schema, self.unique_schema())
        self.assertEqual(relation.schema, self.unique_schema().lower())


class TestCachingLowercaseModel(TestBaseCaching):
    @property
    def models(self):
        return "models"

    def test_iomete_cache(self):
        self.cache_run()


class TestCachingUppercaseModel(TestBaseCaching):
    @property
    def models(self):
        return "shouting_models"

    def test_iomete_cache(self):
        self.cache_run()
