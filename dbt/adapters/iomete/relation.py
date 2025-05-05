from typing import Optional

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy

@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = True
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
