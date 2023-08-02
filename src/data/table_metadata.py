import json
from dataclasses import dataclass


@dataclass
class TableMetadata:
    name: str
    key_col: str
    timestamp_col: str
    type: str
    endpoint: str = None
    schema: dict = None
