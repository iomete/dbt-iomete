from dataclasses import dataclass
from typing import TypeVar, Optional, Dict, Any

from dbt.adapters.spark.column import SparkColumn

Self = TypeVar("Self", bound="IometeColumn")


@dataclass
class IometeColumn(SparkColumn):

    @staticmethod
    def convert_table_stats(raw_stats: Optional[str]) -> Dict[str, Any]:
        table_stats = {}
        if raw_stats:
            # format: 1109049927 bytes, 14093476 rows
            stats = {
                stats.split(" ")[1]: int(stats.split(" ")[0])
                for stats in raw_stats.split(", ")
            }
            for key, val in stats.items():
                table_stats[f"stats:{key}:label"] = key
                table_stats[f"stats:{key}:value"] = val
                table_stats[f"stats:{key}:description"] = ""
                table_stats[f"stats:{key}:include"] = True
        return table_stats
