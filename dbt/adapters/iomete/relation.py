from typing import Optional

from dataclasses import dataclass

from dbt.adapters.spark.relation import SparkRelation



@dataclass(frozen=True, eq=False, repr=False)
class IometeRelation(SparkRelation):
    provider: Optional[str] = None
    is_iceberg: Optional[bool] = None
    describe_table_rows: str = None
