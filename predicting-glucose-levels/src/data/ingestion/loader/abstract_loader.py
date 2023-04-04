from abc import ABC, abstractmethod
import datetime
from typing import List


class AbstractLoader(ABC):
    """
    AbstractLoader is an abstract class that defines logic to load measurements from a data source.

    Attributes:
        ENDPOINTS: A dictionary of endpoints to load data from. The key is the table name, the value is
            a dictionary with the path and timestamp column.
    """

    ENDPOINTS = {
        "entries": {"path": "/entries", "timestamp": "dateString"},
        "treatments": {"path": "/treatments", "timestamp": "created_at"},
    }

    def __init__(self, storage):
        raise NotImplementedError

    def load(self, start: datetime, end: datetime, table: str) -> List:
        """
        Load measurements from a data source between the start and end timestamps. Use the table to
        determine the correct endpoint and timestamp column.
        """
        path, timestamp = self.ENDPOINTS[table].values()
        return self._load(start, end, path, timestamp)

    @abstractmethod
    def _load(self, start: datetime, end: datetime, endpoint: str, timestamp_col: str) -> List:
        """
        Load measurements from a data source between the start and end timestamps. Use the endpoint and
        timestamp to determine the correct endpoint and timestamp column.
        """
        raise NotImplementedError