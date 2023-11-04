from dbt.adapters.iomete.connections import SparkConnectionManager  # noqa
from dbt.adapters.iomete.connections import SparkCredentials  # noqa
from dbt.adapters.iomete.relation import SparkRelation  # noqa
from dbt.adapters.iomete.column import SparkColumn  # noqa
from dbt.adapters.iomete.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import iomete

Plugin = AdapterPlugin(
    adapter=SparkAdapter,
    credentials=SparkCredentials,
    include_path=iomete.PACKAGE_PATH
)
