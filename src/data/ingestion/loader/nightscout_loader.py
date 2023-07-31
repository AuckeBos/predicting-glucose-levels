import os
from datetime import datetime
from typing import List

import requests
from kink import inject

from src.data.ingestion.loader.abstract_loader import AbstractLoader


@inject
class NightscoutLoader(AbstractLoader):
    """
    NightscoutLoader is a class that defines logic to load entires and treatments from a Nightscout API.

    Attributes:
        session: The session to use to connect to the Nightscout API.
        url: The url of the Nightscout API.
    """

    url: str
    session: requests.Session

    def __init__(self, nightscout_uri: str, nightscout_secret: str):
        """
        Create a session with the Nightscout API.
        Read credentials from env
        """
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})
        session.headers.update({"api_secret": nightscout_secret})
        self.session = session

        self.url = nightscout_uri

    def load(
        self, start: datetime, end: datetime, endpoint: str, timestamp_col: str
    ) -> List:
        """
        Load entities from endpoint between start and end timestamps.
        """
        url = f"{self.url}/{endpoint}"
        params = {
            f"find[{timestamp_col}][$gte]": start.isoformat(),
            f"find[{timestamp_col}][$lte]": end.isoformat(),
            "count": 10000000000000,
        }

        response = self.session.get(url, params=params)
        if response.status_code != 200:
            raise Exception(
                f"Error while loading data from Nightscout: {response.text}"
            )
        return response.json()
