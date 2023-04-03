import os
from src.data.ingestion.loader.abstract_loader import AbstractLoader
from datetime import datetime
from typing import List
import requests

class NightscoutLoader(AbstractLoader):

    base_url: str

    def __init__(self):
        """
        Create a session with the Nightscout API.
        Read credentials from env
        """
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})
        secret = os.getenv("NIGHTSCOUT_SECRET")
        session.headers.update({"api_secret": secret})
        self.session = session
        
        self.url = os.getenv("NIGHTSCOUT_URI")



    def load(self, start: datetime, end: datetime) -> List:
        """
        Use the entries endpoint to get all entries between start and end
        The max number of entries is 1000000
        """
        url = f"{self.url}/entries"
        params = {
            "find[dateString][$gte]": start,
            "find[dateString][$lte]": end,
            "count": 1000000
        }

        response = self.session.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f"Error while loading data from Nightscout: {response.text}")
        return response.json()