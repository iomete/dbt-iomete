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
                    'method': 'http',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'password': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        })

    def test_http_connection(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)

        def hive_http_connect(thrift_transport):
            self.assertEqual(thrift_transport.scheme, 'https')
            self.assertEqual(thrift_transport.port, 443)
            self.assertEqual(thrift_transport.host, 'myorg.sparkhost.com')

        # with mock.patch.object(hive, 'connect', new=hive_http_connect):
        with mock.patch('dbt.adapters.iomete.connections.hive.connect', new=hive_http_connect):
            connection = adapter.acquire_connection('dummy')
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, 'open')
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.cluster,
                             '01234-23423-coffeetime')
            self.assertEqual(connection.credentials.password, 'abc123')
            self.assertEqual(connection.credentials.schema, 'analytics')
            self.assertIsNone(connection.credentials.database)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema='default_schema',
            identifier='mytable',
            type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED


        plain_rows = [
            ('col1', 'decimal(22,0)'),
            ('col2', 'string',),
            ('dt', 'date'),
            ('struct_col', 'struct<struct_inner_col:string>'),
            ('# Partition Information', 'data_type'),
            ('# col_name', 'data_type'),
            ('dt', 'date'),
            (None, None),
            ('# Detailed Table Information', None),
            ('Database', None),
            ('Owner', 'root'),
            ('Created Time', 'Wed Feb 04 18:15:00 UTC 1815'),
            ('Last Access', 'Wed May 20 19:25:00 UTC 1925'),
            ('Type', 'MANAGED'),
            ('Provider', 'parquet'),
            ('Location', '/mnt/vo'),
            ('Serde Library', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
            ('InputFormat', 'org.apache.hadoop.mapred.SequenceFileInputFormat'),
            ('OutputFormat', 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            ('Partition Provider', 'Catalog')
        ]

        input_cols = [Row(keys=['col_name', 'data_type'], values=r)
                      for r in plain_rows]

        config = self._get_target_http(self.project_cfg)
        rows = SparkAdapter(config).parse_describe_extended(
            relation, input_cols)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'col1',
            'column_index': 0,
            'dtype': 'decimal(22,0)',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[1].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'col2',
            'column_index': 1,
            'dtype': 'string',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[2].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'dt',
            'column_index': 2,
            'dtype': 'date',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[3].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'struct_col',
            'column_index': 3,
            'dtype': 'struct<struct_inner_col:string>',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema='default_schema',
            identifier='mytable',
            type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ('col1', 'decimal(22,0)'),
            ('# Detailed Table Information', None),
            ('Owner', 1234)
        ]

        input_cols = [Row(keys=['col_name', 'data_type'], values=r)
                      for r in plain_rows]

        config = self._get_target_http(self.project_cfg)
        rows = SparkAdapter(config).parse_describe_extended(
            relation, input_cols)

        self.assertEqual(rows[0].to_column_dict().get('table_owner'), '1234')

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema='default_schema',
            identifier='mytable',
            type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ('col1', 'decimal(22,0)'),
            ('# Partition Information', 'data_type'),
            (None, None),
            ('# Detailed Table Information', None),
            ('Database', None),
            ('Owner', 'root'),
            ('Created Time', 'Wed Feb 04 18:15:00 UTC 1815'),
            ('Last Access', 'Wed May 20 19:25:00 UTC 1925'),
            ('Statistics', '1109049927 bytes, 14093476 rows'),
            ('Type', 'MANAGED'),
            ('Provider', 'iceberg'),
            ('Location', '/mnt/vo'),
            ('Serde Library', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
            ('InputFormat', 'org.apache.hadoop.mapred.SequenceFileInputFormat'),
            ('OutputFormat', 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            ('Partition Provider', 'Catalog')
        ]

        input_cols = [Row(keys=['col_name', 'data_type'], values=r)
                      for r in plain_rows]

        config = self._get_target_http(self.project_cfg)
        rows = SparkAdapter(config).parse_describe_extended(
            relation, input_cols)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'col1',
            'column_index': 0,
            'dtype': 'decimal(22,0)',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None,

            'stats:bytes:description': '',
            'stats:bytes:include': True,
            'stats:bytes:label': 'bytes',
            'stats:bytes:value': 1109049927,

            'stats:rows:description': '',
            'stats:rows:include': True,
            'stats:rows:label': 'rows',
            'stats:rows:value': 14093476,
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

    def test_parse_relation_from_cache(self):
        self.maxDiff = None
        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED

        plain_rows = [
            ('col1', 'decimal(22,0)'),
            ('col2', 'string',),
            ('dt', 'date'),
            ('struct_col', 'struct<struct_inner_col:string>'),
            ('# Partition Information', 'data_type'),
            ('# col_name', 'data_type'),
            ('dt', 'date'),
            (None, None),
            ('# Detailed Table Information', None),
            ('Database', None),
            ('Owner', 'root'),
            ('Created Time', 'Wed Feb 04 18:15:00 UTC 1815'),
            ('Last Access', 'Wed May 20 19:25:00 UTC 1925'),
            ('Type', 'MANAGED'),
            ('Provider', 'parquet'),
            ('Location', '/mnt/vo'),
            ('Serde Library', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
            ('InputFormat', 'org.apache.hadoop.mapred.SequenceFileInputFormat'),
            ('OutputFormat', 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            ('Partition Provider', 'Catalog')
        ]

        input_cols = [Row(keys=['col_name', 'data_type'], values=r)
                      for r in plain_rows]

        describe_table_rows = [dict(zip(row._keys, row._values)) for row in input_cols]

        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema='default_schema',
            identifier='mytable',
            type=rel_type,
            describe_table_rows=describe_table_rows,
            is_iceberg="iceberg"
        )

        config = self._get_target_http(self.project_cfg)
        rows = SparkAdapter(config).parse_describe_extended_from_describe_table_rows(
            relation, relation.describe_table_rows)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'col1',
            'column_index': 0,
            'dtype': 'decimal(22,0)',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[1].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'col2',
            'column_index': 1,
            'dtype': 'string',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[2].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'dt',
            'column_index': 2,
            'dtype': 'date',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })

        self.assertEqual(rows[3].to_column_dict(omit_none=False), {
            'table_database': None,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'table_owner': 'root',
            'column': 'struct_col',
            'column_index': 3,
            'dtype': 'struct<struct_inner_col:string>',
            'numeric_scale': None,
            'numeric_precision': None,
            'char_size': None
        })