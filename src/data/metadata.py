import json
from dataclasses import dataclass
from typing import List

from src.data.table_metadata import TableMetadata
from src.helpers.config import METADATA_DIR


class Metadata:
    tables: List[TableMetadata]

    def __init__(self) -> None:
        self.tables = [
            TableMetadata(**json.load(open(f)))
            for f in (METADATA_DIR / "tables").glob("*.json")
        ]

    def get_table(self, table: str) -> TableMetadata:
        """
        Get the metadata for a table.
        """
        try:
            return next(x for x in self.tables if x.name == table)
        except StopIteration:
            raise Exception(f"Table {table} not found in metadata.")
