from abc import abstractmethod, ABC
from typing import List, Tuple
from datetime import datetime

class AbstractStorage(ABC):
    """
    AbstractStorage is an abstract class that defines the interface for a storage class.

    Attributes:
        MEASUREMENTS_TABLE: The name of the measurements table.
        RUNMOMENTS_TABLE: The name of the runmoments table.
    """
    MEASUREMENTS_TABLE = "measurements"
    RUNMOMENTS_TABLE = "runmoments"

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
    def get_last_runmoment(self) -> datetime:
        """
        Get the last timestamp from the runmoments table. This is used to determine the window of data
        to load.
        """
        raise NotImplementedError

    @abstractmethod
    def set_last_runmoment(self, timestamp: datetime) -> None:
        """
        Set the last timestamp in the runmoments table.
        """
        raise NotImplementedError

    def upsert_measurements(self, data: List) -> None:
        """
        Upsert a list of measurements into the measurements table.
        """
        return self.upsert(data, self.MEASUREMENTS_TABLE, "_id", "date")

    def get_window(self) -> Tuple[datetime, datetime]:
        """
        Get the window of data to load. This is the last timestamp in the runmoments table and the
        current time.
        """
        start = self.get_last_runmoment()
        end = datetime.now()
        return start, end