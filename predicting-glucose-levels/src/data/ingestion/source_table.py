from dataclasses import dataclass


@dataclass
class SourceTable:
    endpoint: str
    destination_name: str
    key_col: str
    timestamp_col: str
