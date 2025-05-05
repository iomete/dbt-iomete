import unittest

import dbt.flags as flags
from dbt.exceptions import DbtProfileError, DbtRuntimeError
from dbt.adapters.iomete import SparkAdapter
from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = False

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': False,
            },
            'config-version': 2
        }

    def _get_target_http(self, project):
        return config_from_parts_or_dicts(project, {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'host': 'iomete.com',
                    'dataplane': 'spark-resource',
                    'domain': 'default',
                    'lakehouse': 'dbt',
                    'user': 'user1',
                    'token': 'abc123',
                    'port': 443,
                    'schema': 'analytics'
                }
            },
            'target': 'test'
        })

    def test_relation_with_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)

        adapter.Relation.create(schema='different', identifier='table')
        relation = adapter.Relation.create(database='something', schema='different', identifier='table')
        self.assertIsNotNone(relation)

    def test_relation_without_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)

        relation = adapter.Relation.create(schema='different', identifier='table')
        self.assertIsNotNone(relation)

    def test_profile_with_database_keyword(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config)

        self.assertEqual(adapter.config.credentials.database, 'demo_catalog')

    def test_profile_with_catalog_keyword(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'catalog': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config)

        self.assertEqual(adapter.config.credentials.database, 'demo_catalog')

    def test_profile_with_both_database_and_catalog(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'catalog': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtProfileError):
            config_from_parts_or_dicts(self.project_cfg, profile)

    def test_profile_with_empty_database(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': '',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.project_cfg, profile)

    def test_profile_with_incorrect_schema_containing_catalog_using_dot_notation(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'demo_catalog.analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.project_cfg, profile)
