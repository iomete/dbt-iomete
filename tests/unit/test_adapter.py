import unittest
from unittest import mock

import dbt.flags as flags
from dbt.exceptions import RuntimeException
from agate import Row
from pyhive import hive
from dbt.adapters.iomete import SparkAdapter, SparkRelation
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
                    'lakehouse': 'dbt',
                    'account_number': 'analytics',
                    'user': 'user1',
                    'password': 'abc123',
                    'api_token': 'abc123',
                    'port': 443,
                    'schema': 'analytics'
                }
            },
            'target': 'test'
        })

    def test_relation_with_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)
        # fine
        adapter.Relation.create(schema='different', identifier='table')
        with self.assertRaises(RuntimeException):
            # not fine - database set
            adapter.Relation.create(
                database='something', schema='different', identifier='table')

    def test_profile_with_database(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    # not allowed
                    'database': 'analytics2',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'password': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }
        with self.assertRaises(RuntimeException):
            config_from_parts_or_dicts(self.project_cfg, profile)