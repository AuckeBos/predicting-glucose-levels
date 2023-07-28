from abc import ABC, abstractmethod
import datetime
from typing import List


class AbstractLoader(ABC):
    """
    AbstractLoader is an abstract class that defines logic to load measurements from a data source.
    """

    def __init__(self, storage):
        raise NotImplementedError

    @abstractmethod
    def load(
        self, start: datetime, end: datetime, endpoint: str, timestamp_col: str
    ) -> List:
        """
        Load measurements from a data source between the start and end timestamps. Use the endpoint and
        timestamp to determine the correct endpoint and timestamp column.
        """
        raise NotImplementedError
