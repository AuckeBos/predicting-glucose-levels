from dataclasses import dataclass


@dataclass
class SourceTable:
    endpoint: str
    key_col: str
    timestamp_col: str
    destination_name: str
