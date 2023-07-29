from abc import abstractmethod, ABC
from typing import List, Optional, Tuple
from datetime import datetime


class AbstractStorage(ABC):
    """
    AbstractStorage is an abstract class that defines the interface for a storage class.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Upsert a list of rows into a table. Use the key_col to identify the row and the timestamp_col to
        determine the order of the rows.
        """
        raise NotImplementedError

    @abstractmethod
    def insert(self, data: List, table: str) -> None:
        """
        Insert a list of rows into a table.
        """
        raise NotImplementedError

    @abstractmethod
    def find(self, table: str, query: dict, sort: List[str], asc: bool = True) -> List:
        """
        Find rows in a table that match the query.

        Parameters:
            table: The name of the table to query.
            query: A dictionary of key=value pairs.
            sort: A list of columns to sort by.
            asc: Whether to sort ascending or descending.
        """
        raise NotImplementedError

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
        return result["timestamp"] if result else datetime(2022, 1, 1)

    def set_last_runmoment(self, source: str, timestamp: datetime) -> None:
        """
        Set the last timestamp in the runmoments table. Use the upsert method.
        """
        data = [{"source": source, "timestamp": timestamp}]
        self.upsert(data, "runmoments", "source", "timestamp")

    def get_window(self, source: str) -> Tuple[datetime, datetime]:
        """
        Get the window of data to load. This is the last timestamp in the runmoments table and the
        current time.
        """
        start = self.get_last_runmoment(source)
        end = datetime.now()
        return start, end
