import json
from typing import List

from kink import inject

from src.data.table_metadata import TableMetadata


@inject
class Metadata:
    tables: List[TableMetadata]

    def __init__(self, metadata_dir: str) -> None:
        self.tables = [
            TableMetadata(**json.load(open(f)))
            for f in (metadata_dir / "tables").glob("*.json")
        ]

    def get_table(self, table: str) -> TableMetadata:
        """
        Get the metadata for a table.
        """
        # print(self.tables)
        try:
            return next(x for x in self.tables if x.name == table)
        except StopIteration:
            raise Exception(f"Table {table} not found in metadata.")
