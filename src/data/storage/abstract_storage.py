from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd

from src.helpers.general import now


class AbstractStorage(ABC):
    """
    AbstractStorage is an abstract class that defines the interface for a storage class.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Upsert a list of rows into a table. Use the key_col to identify the row and the timestamp_col to
        determine the order of the rows.
        """
        raise NotImplementedError

    @abstractmethod
    def _insert(self, data: List, table: str) -> None:
        """
        Insert a list of rows into a table.
        """
        raise NotImplementedError

    @abstractmethod
    def _overwrite(self, data: pd.DataFrame, table: str) -> None:
        """
        Overwrite the full contents of a table with the data.
        """
        raise NotImplementedError

    @abstractmethod
    def find(
        self, table: str, query: dict = None, sort: List[str] = None, asc: bool = True
    ) -> List:
        """
        Find rows in a table that match the query.

        Parameters:
            table: The name of the table to query.
            query: A dictionary of key=value pairs.
            sort: A list of columns to sort by.
            asc: Whether to sort ascending or descending.
        """
        raise NotImplementedError

    def get(self, table: str, as_dataframe: bool = False) -> List:
        """
        Get all rows in a table.

        Parameters:
            table: The name of the table to query.
        """
        result = self.find(table)
        if as_dataframe:
            result = pd.DataFrame(result)
        return result

    def overwrite(self, data: pd.DataFrame, table: str) -> None:
        """
        Overwrite the full contents of a table with a dataframe.
        """
        self._overwrite(data, table)

    def upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Add updated_at, and then call _upsert.
        """
        updated_at = now().isoformat()
        data = map(lambda x: {**x, "updated_at": updated_at}, data)
        self._upsert(data, table, key_col, timestamp_col)

    def insert(self, data: List, table: str) -> None:
        """
        Add inserted_at, and then call _insert.
        """
        inserted_at = now().isoformat()
        data = map(lambda x: {**x, "inserted_at": inserted_at}, data)
        self._insert(data, table)

    def find_one(
        self, table: str, query: dict, sort: List[str] = [], asc: bool = True
    ) -> Optional[dict]:
        """
        Find one row in a table that matches the query.
        """
        result = self.find(table, query, sort, asc)
        return result[0] if result else None

    def get_last_runmoment(self, source: str) -> datetime:
        """
        Get the last timestamp from the runmoments table. This is used to determine the window of data
        to load. Return 2022-01-01 if there is no timestamp in the runmoments table.
        """
        result = self.find_one("runmoments", {"source": source})
        return (
            datetime.fromisoformat(result["timestamp"])
            if result
            else datetime(2020, 1, 1)
        )

    def set_last_runmoment(self, source: str, timestamp: datetime) -> None:
        """
        Set the last timestamp in the runmoments table. Use the upsert method.
        """
        data = [{"source": source, "timestamp": timestamp.isoformat()}]
        self.upsert(data, "runmoments", "source", "timestamp")

    def get_window(self, source: str) -> Tuple[datetime, datetime]:
        """
        Get the window of data to load. This is the last timestamp in the runmoments table and the
        current time.
        """
        start = self.get_last_runmoment(source)
        end = datetime.now()
        return start, end
