import dbt.exceptions
from dbt.adapters.spark import SparkCredentials
from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark.connections import PyhiveConnectionWrapper
from dbt.contracts.connection import ConnectionState
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS

try:
    from thrift.transport import THttpClient
    from pyhive import hive
except ImportError:
    THttpClient = None
    hive = None
try:
    import pyodbc
except ImportError:
    pyodbc = None
from datetime import datetime

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
try:
    from thrift.transport.TSSLSocket import TSSLSocket
    import thrift
    import ssl
except ImportError:
    TSSLSocket = None
    thrift = None
    ssl = None
    sasl = None

import base64
import time

logger = AdapterLogger("Iomete")

NUMBERS = DECIMALS + (int, float)


@dataclass
class IometeCredentials(SparkCredentials):
    host: str
    database: Optional[str]
    cluster: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 443
    connect_retries: int = 0
    connect_timeout: int = 120
    server_side_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False

    def __post_init__(self):
        # spark classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt.exceptions.RuntimeException(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

    @property
    def type(self):
        return "iomete"

    def _connection_keys(self):
        return "host", "port", "cluster", "schema"


class IometeConnectionManager(SparkConnectionManager):
    TYPE = "iomete"

    @classmethod
    def validate_creds(cls, creds, required):
        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError("The config '{}' is required to connect to Spark".format(key))

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None

        SPARK_IOMETE_CONNECTION_URL = (
            "https://{host}:{port}/{cluster}/cliservice"
        )

        for i in range(1 + creds.connect_retries):
            try:
                cls.validate_creds(creds, ["host", "port", "user", "password", "cluster"])

                conn_url = SPARK_IOMETE_CONNECTION_URL.format(
                    host=creds.host,
                    port=creds.port,
                    cluster=creds.cluster
                )
                logger.debug("connection url: {}".format(conn_url))
                transport = THttpClient.THttpClient(conn_url)
                credentials = "%s:%s" % (creds.user, creds.password)
                transport.setCustomHeaders(
                    {"Authorization": "Basic " + base64.b64encode(credentials.encode()).decode().strip()})

                conn = hive.connect(thrift_transport=transport)
                handle = PyhiveConnectionWrapper(conn)
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a password is invalid, or something
                    msg = "Failed to connect"
                    if creds.password is not None:
                        msg += ", is your password valid?"
                    raise dbt.exceptions.FailedToConnectException(msg) from e
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
                    raise dbt.exceptions.FailedToConnectException("failed to connect") from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection


def _is_retryable_error(exc: Exception) -> Optional[str]:
    message = getattr(exc, "message", None)
    if message is None:
        return None
    message = message.lower()
    if "pending" in message:
        return exc.message
    if "temporarily_unavailable" in message:
        return exc.message
    return None
