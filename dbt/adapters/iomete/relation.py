from typing import Optional

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import DbtRuntimeError


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: SparkIncludePolicy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: SparkIncludePolicy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = '`'
    provider: Optional[str] = None
    is_iceberg: Optional[bool] = None
    describe_table_rows: str = None
    table_fields: list = None

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise DbtRuntimeError('Cannot set database in spark!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                'Got a spark relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()
