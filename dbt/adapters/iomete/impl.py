from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Iterable, Type
import agate
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.relation import RelationType
from dbt.context.exceptions_jinja import raise_compiler_error

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig, PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.iomete import SparkConnectionManager
from dbt.adapters.iomete import SparkRelation
from dbt.adapters.iomete import SparkColumn
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt_common.utils import AttrDict
from dbt.events import AdapterLogger
from dbt.utils import executor
import sentry_sdk

from dbt.adapters.iomete.python_job import IometeSparkJobHelper
from dbt.adapters.iomete.schema_service import SchemaService

logger = AdapterLogger("iomete")

LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
FETCH_TBL_PROPERTIES_MACRO_NAME = 'fetch_tbl_properties'
GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"

KEY_TABLE_OWNER = 'Owner'
KEY_TABLE_STATISTICS = 'Statistics'

sentry_sdk.init(
    dsn="https://a1424d21130340e4913bd8bc1b228c12@o1140336.ingest.sentry.io/4504214031695872",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0,
    attach_stacktrace=True
)


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

    def __init__(self, config):
        super().__init__(config)
        self.schema_service = SchemaService(credentials=config.credentials)

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

    def list_relations_without_caching(
            self, schema_relation: SparkRelation
    ) -> List[SparkRelation]:
        tables = self.schema_service.get_tables_by_namespace(schema_relation.database, schema_relation.schema)

        relations = []
        for table in tables:
            rel_type = RelationType.Table
            if table['type'] and table['type'].lower() == 'view':
                rel_type = RelationType.View

            provider = table['provider'].lower() if table['provider'] else None

            relation = self.Relation.create(
                database=table['catalog'],
                schema=table['namespace'],
                identifier=table['name'],
                type=rel_type,
                provider=provider,
                is_iceberg=provider == "iceberg",
                table_fields=table["columns"],
            )
            relations.append(relation)

        return relations

    def get_relation(
            self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore

        return super().get_relation(database, schema, identifier)

    def get_columns_in_relation(self, relation: Relation) -> List[SparkColumn]:
        is_temp_table = relation.schema is None and relation.identifier.endswith("tmp")
        if is_temp_table:
            return self._get_columns_of_temp_table(relation)

        rows: AttrDict = self.execute_macro(
            GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME, kwargs={"relation": relation}
        )
        columns = self.parse_describe_extended(relation, rows)
        return columns

    def _get_columns_of_temp_table(self, relation: Relation) -> List[SparkColumn]:
        try:
            desc_table_columns = self.execute_macro(
                'describe_temp_view',
                kwargs={"relation": relation}
            )

            return [SparkColumn(
                table_database= relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=None,
                table_stats=None,
                column=column['col_name'],
                column_index=idx,
                dtype=column['data_type'],
            ) for idx, column in enumerate(desc_table_columns)]
        except dbt.exceptions.DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            if "Table or view not found" in errmsg or "NoSuchTableException" in errmsg:
                return []
            else:
                raise e

    def _get_columns_for_catalog(
            self, relation: SparkRelation
    ) -> Iterable[Dict[str, Any]]:
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = column.table_database
            yield as_dict

    def get_catalog(self, manifest):

        schema_map = self._get_catalog_schemas(manifest)

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
        if len(schemas) != 1:
            raise_compiler_error(
                f'Expected only one schema in spark _get_one_catalog, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
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

    def parse_describe_extended(
        self, relation: BaseRelation, raw_rows: AttrDict
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        return [
            SparkColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=None,
                table_stats=None,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "spark_job": IometeSparkJobHelper,
        }

    @property
    def default_python_submission_method(self) -> str:
        return "spark_job"

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    # In original implementation handle.commit() and handle.rollback() are called.
    # Since spark doesn't support transactions, this implementation is a no-op.
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                if hasattr(cursor, "fetchone"):
                    return cursor.fetchone()
                else:
                    # AttributeError: 'PyhiveConnectionWrapper' object has no attribute 'fetchone'
                    return cursor.fetchall()[0]
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row["col_name"] or row["col_name"].startswith("#"):
                break
            pos += 1
        return pos


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
