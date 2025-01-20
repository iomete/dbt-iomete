import requests
import json
from typing import Optional
from dbt.context.exceptions_jinja import raise_compiler_error
from requests.adapters import HTTPAdapter
from urllib3 import Retry

IOMETE_DEFAULT_CATALOG_NAME = "spark_catalog"

class SchemaService:
    def __init__(self, credentials):
        self.credentials = credentials

        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5, allowed_methods=None,
                                                status_forcelist=[429, 500, 502, 503, 504]))
        self.session = requests.Session()
        self.session.mount(credentials.scheme, adapter)

    def get_tables_by_namespace(self, namespace: str) -> list:
        final_namespace = namespace.split(".").pop()

        return self._get_namespaces(
            path=f"{final_namespace}/tables?includeMetadata=true",
            error_message=f"Could not get tables for schema {final_namespace}") or []

    def get_table(self, namespace: str, table_name: str) -> Optional[dict]:
        final_namespace = namespace.split(".").pop()

        return self._get_namespaces(
            path=f"{final_namespace}/tables/{table_name}",
            error_message=f"Could not get table metadata for {final_namespace}.{table_name}")

    def _get_namespaces(self, path: str, error_message: str):
        try:
            catalog = self.credentials.schema.split(".")[0] if "." in self.credentials.schema else IOMETE_DEFAULT_CATALOG_NAME
            namespaces = f"{self.credentials.scheme}://{self.credentials.host}:{self.credentials.port}/api/v1/domains/{self.credentials.domain}/schema/catalogs/{catalog}/namespaces"

            response = self.session.get(f"{namespaces}/{path}", timeout=10,
                                        headers={"X-API-TOKEN": self.credentials.token})
            if response.status_code == 404:
                return None

            response.raise_for_status()
            return json.loads(response.text)
        except requests.exceptions.HTTPError as err:
            raise_compiler_error(
                f"{error_message}. "
                f"Request failed with status: {err.response.status_code} and error message is: {err.response.text}"
            )
        except requests.exceptions.RequestException as e:
            raise_compiler_error(f"{error_message}. Request failed with error: {e}")
