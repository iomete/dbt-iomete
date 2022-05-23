from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Iterable
import agate
from dbt.contracts.relation import RelationType

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.iomete import SparkConnectionManager
from dbt.adapters.iomete import SparkRelation
from dbt.adapters.iomete import SparkColumn
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.events import AdapterLogger
from dbt.utils import executor

logger = AdapterLogger("iomete")

GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
FETCH_TBL_PROPERTIES_MACRO_NAME = 'fetch_tbl_properties'

KEY_TABLE_OWNER = 'Owner'
KEY_TABLE_STATISTICS = 'Statistics'


@dataclass
class SparkConfig(AdapterConfig):
    file_format: str = 'parquet'
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class SparkAdapter(SQLAdapter):
    Relation = SparkRelation
    Column = SparkColumn
    ConnectionManager = SparkConnectionManager
    AdapterSpecificConfigs = SparkConfig

    @classmethod
    def date_function(cls) -> str:
        return 'current_timestamp()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    def quote(self, identifier):
        return '`{}`'.format(identifier)

    def add_schema_to_cache(self, schema) -> str:
        """Cache a new schema in dbt. It will show up in `list relations`."""
        if schema is None:
            name = self.nice_connection_name()
            dbt.exceptions.raise_compiler_error(
                'Attempted to cache a null schema for {}'.format(name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ''

    def list_relations_without_caching(
            self, schema_relation: SparkRelation
    ) -> List[SparkRelation]:
        kwargs = {'schema_relation': schema_relation}
        try:
            results = self.execute_macro(
                'list_all_relations_without_caching',
                kwargs=kwargs
            )

            view_result = self.execute_macro(
                'list_views_relations_without_caching',
                kwargs=kwargs
            )

            view_set = set([schema + "." + name for schema, name, _ in view_result])
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 3:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "show tables...", '
                    f'got {len(row)} values, expected 3'
                )
            _schema, name, is_temporary = row

            rel_type = RelationType.View \
                if _schema + "." + name in view_set else RelationType.Table

            tmp_relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type
            )

            describe_table_result = self.execute_macro(
                GET_COLUMNS_IN_RELATION_MACRO_NAME,
                kwargs={'relation': tmp_relation}
            )

            describe_table_rows = [dict(zip(row._keys, row._values)) for row in describe_table_result]
            provider = self.get_provider(describe_table_rows)

            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type,
                provider=provider,
                is_iceberg= provider == "iceberg",
                describe_table_rows=describe_table_rows
            )

            relations.append(relation)

        return relations

    def get_relation(
            self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def parse_describe_extended(
            self,
            relation: Relation,
            raw_rows: List[agate.Row]
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]

        return self.parse_describe_extended_from_describe_table_rows(relation, dict_rows)

    def parse_describe_extended_from_describe_table_rows(
            self,
            relation: Relation,
            describe_table_rows: List[dict]
    ) -> List[SparkColumn]:

        pos = self.find_table_information_separator(describe_table_rows)

        # Remove rows that start with a hash, they are comments
        rows = [
            row for row in describe_table_rows[0:pos]
            if not row['col_name'].startswith('#')
        ]

        metadata_position = self.find_table_metadata_separator(describe_table_rows)
        metadata = {
            col['col_name']: col['data_type'] for col in describe_table_rows[metadata_position + 1:]
        }

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        return [SparkColumn(
            table_database=None,
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=str(metadata.get(KEY_TABLE_OWNER)),
            table_stats=table_stats,
            column=column['col_name'],
            column_index=idx,
            dtype=column['data_type'],
        ) for idx, column in enumerate(rows)]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row['col_name'] or row['col_name'].startswith('#'):
                break
            pos += 1
        return pos

    @staticmethod
    def find_table_metadata_separator(rows: List[dict]) -> int:
        last_index = len(rows) - 1
        for pos in range(last_index, -1, -1):
            row = rows[pos]
            if not row['col_name'] or row['col_name'].startswith('#'):
                return pos
        return 0

    @staticmethod
    def get_provider(rows: List[dict]) -> Optional[str]:
        for row in rows:
            if row['col_name'] == 'Provider':
                return row['data_type']
        return None

    def get_columns_in_relation(self, relation: Relation) -> List[SparkColumn]:
        cached_relations = self.cache.get_relations(relation.database, relation.schema)
        cached_relation = next(
            (
                cached_relation
                for cached_relation in cached_relations
                if str(cached_relation) == str(relation)
            ),
            None,
        )
        columns = []
        if cached_relation and cached_relation.describe_table_rows:
            columns = self.parse_describe_extended_from_describe_table_rows(
                cached_relation, cached_relation.describe_table_rows
            )
        if not columns:
            try:
                rows: List[agate.Row] = super().get_columns_in_relation(relation)
                columns = self.parse_describe_extended(relation, rows)
            except dbt.exceptions.RuntimeException as e:
                # spark would throw error when table doesn't exist, where other
                # CDW would just return and empty list, normalizing the behavior here
                errmsg = getattr(e, "msg", "")
                if "Table or view not found" in errmsg or "NoSuchTableException" in errmsg:
                    pass
                else:
                    raise e

        return columns

    def _get_columns_for_catalog(
            self, relation: SparkRelation
    ) -> Iterable[Dict[str, Any]]:
        logger.warning("_get_columns_for_catalog {}", relation.__dict__)
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None
            yield as_dict

    def get_properties(self, relation: Relation) -> Dict[str, str]:
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME,
            kwargs={'relation': relation}
        )
        return dict(properties)

    def get_catalog(self, manifest):

        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        logger.warning("get_catalog1 {}", schema_map)
        logger.warning("get_catalog2 {}", self.config)

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], manifest
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
            self, information_schema, schemas, manifest,
    ) -> agate.Table:
        logger.warning("_get_one_catalog1 {}", information_schema.__dict__)
        logger.warning("_get_one_catalog2 {}", schemas)
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in spark _get_one_catalog, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", relation)
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(
            columns, column_types=DEFAULT_TYPE_TESTER
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_rows_different_sql(
            self,
            relation_a: BaseRelation,
            relation_b: BaseRelation,
            column_names: Optional[List[str]] = None,
            except_operator: str = 'EXCEPT',
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = '''
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
cross join diff_count
'''.strip()
