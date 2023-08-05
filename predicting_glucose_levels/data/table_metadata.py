from dataclasses import dataclass


@dataclass
class TableMetadata:
    """
    A class that contains metadata about a table.
    """

    name: str
    key_col: str
    timestamp_col: str
    type: str
    endpoint: str = None
    schema: dict = None
