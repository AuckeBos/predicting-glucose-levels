from dataclasses import dataclass

@dataclass
class SourceTable:
    source_name: str
    destination_name: str
    key_col: str
    timestamp_col: str
    
