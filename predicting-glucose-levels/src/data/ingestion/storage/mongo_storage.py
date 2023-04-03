import os
from typing import List
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from src.data.ingestion.storage.abstract_storage import AbstractStorage
from datetime import datetime

class MongoStorage(AbstractStorage):
    """
    The MongoStorage class is used to store data in a MongoDB database.
    """

    client: MongoClient
    db: Database
    measurements: Collection
    runmoments: Collection

    def __init__(self):
        """
        Initialize the MongoStorage class.
        Read credentials from env.
        """
        self.client = MongoClient(
            os.getenv("MONGO_URI"),
            username=os.getenv("MONGO_USER"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        self.db = self.client[os.getenv("MONGO_DB")]
        self.measurements = self.db[self.MEASUREMENTS_TABLE]
        self.runmoments = self.db[self.RUNMOMENTS_TABLE]

    
    def get_last_runmoment(self) -> datetime:
        """
        Get the last value from the runmoments table. Otherwise return 01-01-2023.
        """
        if runmoment := self.runmoments.find_one(sort=[("timestamp", -1)]):
            return runmoment["timestamp"]
        return datetime(2023, 1, 1)

    def set_last_runmoment(self, timestamp: datetime) -> None:
        """
        Append the timestamp to the runmoments table.
        """
        self.runmoments.insert_one({"timestamp": timestamp})

    def upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Upsert each item. For now, simply loop over them and insert each one separately.
        """
        table = self.db[table]
        for row in data:
            table.update_one({key_col: row[key_col]}, {"$set": row}, upsert=True)