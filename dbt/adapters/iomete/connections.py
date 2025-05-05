from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS

from TCLIService.ttypes import TOperationState as ThriftState
from pyhive import hive

from datetime import datetime

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import time

logger = AdapterLogger("Spark")

NUMBERS = DECIMALS + (int, float)
IOMETE_DEFAULT_CATALOG_NAME = "spark_catalog"


@dataclass
class SparkCredentials(Credentials):
    database: Optional[str] = None  # type: ignore
    schema: Optional[str] = None  # type: ignore
    https: bool = True
    host: Optional[str] = None
    port: int = 443
    dataplane: Optional[str] = None
    domain: Optional[str] = None
    lakehouse: Optional[str] = None
    user: Optional[str] = None
    token: Optional[str] = None
    connect_retries: int = 0
    connect_timeout: int = 120
    server_side_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False

    _ALIASES = {
        'catalog': 'database',
    }

    def __post_init__(self):
        if self.database is not None and not self.database.strip():
            raise dbt.exceptions.ValidationError(f"Invalid catalog name : {self.database}.")
        if self.database is None:
            self.database = IOMETE_DEFAULT_CATALOG_NAME

        if "." in (self.schema or ""):
            raise dbt.exceptions.ValidationError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use `catalog` instead.\n"
            )
        return

    @property
    def type(self):
        return 'iomete'

    @property
    def scheme(self):
        return 'https' if self.https else 'http'

    @property
    def unique_field(self):
        return f"{self.scheme}://{self.host}:{self.port}/dataplane/{self.dataplane}/lakehouse/{self.lakehouse}"

    def _connection_keys(self):
        return 'host', 'port','dataplane', 'lakehouse', 'database', 'schema'


class PyhiveConnectionWrapper(object):
    """Wrap a Spark connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while cancelling query: {}".format(exc)
                )

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while closing cursor: {}".format(exc)
                )
        self.handle.close()

    def rollback(self, *args, **kwargs):
        pass

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Reaching into the private enumeration here is bad form,
        # but there doesn't appear to be any way to determine that
        # a query has completed executing from the pyhive public API.
        # We need to use an async query + poll here, otherwise our
        # request may be dropped after ~5 minutes by the thrift server
        STATE_PENDING = [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]

        STATE_SUCCESS = [
            ThriftState.FINISHED_STATE,
        ]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        self._cursor.execute(sql, bindings, async_=True)
        poll_state = self._cursor.poll()
        state = poll_state.operationState

        while state in STATE_PENDING:
            logger.debug("Poll status: {}, sleeping".format(state))

            poll_state = self._cursor.poll()
            state = poll_state.operationState

        # If an errorMessage is present, then raise a database exception
        # with that exact message. If no errorMessage is present, the
        # query did not necessarily succeed: check the state against the
        # known successful states, raising an error if the query did not
        # complete in a known good state. This can happen when queries are
        # cancelled, for instance. The errorMessage will be None, but the
        # state of the query will be "cancelled". By raising an exception
        # here, we prevent dbt from showing a status of OK when the query
        # has in fact failed.
        if poll_state.errorMessage:
            logger.debug("Poll response: {}".format(poll_state))
            logger.debug("Poll status: {}".format(state))
            raise dbt.exceptions.DbtDatabaseError(poll_state.errorMessage)

        elif state not in STATE_SUCCESS:
            status_type = ThriftState._VALUES_TO_NAMES.get(
                state,
                'Unknown<{!r}>'.format(state))

            raise dbt.exceptions.DbtDatabaseError(
                "Query failed with status: {}".format(status_type))

        logger.debug("Poll status: {}, query complete".format(state))

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
           the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class SparkConnectionManager(SQLConnectionManager):
    TYPE = 'iomete'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, 'status'):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.DbtRuntimeError(msg)
            else:
                raise dbt.exceptions.DbtRuntimeError(str(exc))

    def cancel(self, connection):
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = 'OK'
        return AdapterResponse(
            _message=message
        )

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def rollback(self, *args, **kwargs):
        pass

    @classmethod
    def validate_creds(cls, creds, required):
        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(f"The config '{key}' is required to connect to iomete")

            if creds.__dict__[key] is None:
                raise dbt.exceptions.DbtProfileError(
                    f"The config '{key}' is set to none! This config is required to connect to iomete")

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug('Connection is already open, skipping open.')
            return connection

        creds = connection.credentials
        exc = None

        for i in range(1 + creds.connect_retries):
            try:
                cls.validate_creds(creds, ['host', 'port', 'user', 'token', 'lakehouse', 'dataplane'])
                conn = hive.connect(
                    scheme=creds.scheme,
                    host=creds.host,
                    port=creds.port,
                    lakehouse=creds.lakehouse,
                    database=creds.database,
                    username=creds.user,
                    password=creds.token,
                    data_plane=creds.dataplane
                )
                handle = PyhiveConnectionWrapper(conn)
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a password is invalid, or something
                    msg = 'Failed to connect. Make sure lakehouse is in non-terminated state ' \
                          'and credentials (user/password) are correct'
                    raise dbt.exceptions.FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectError(
                        'Failed to connect! Make sure host, port, protocol (https/http) is correct!'
                    ) from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection


def _is_retryable_error(exc: Exception) -> Optional[str]:
    message = getattr(exc, 'message', None)
    if message is None:
        return None
    message = message.lower()
    if 'pending' in message:
        return exc.message
    if 'temporarily_unavailable' in message:
        return exc.message
    return None
