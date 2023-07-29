import os
from typing import List
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from src.data.storage.abstract_storage import AbstractStorage
from datetime import datetime


class MongoStorage(AbstractStorage):
    """
    The MongoStorage class is used to store data in a MongoDB database.

    Attributes:
        client: The MongoDB client.
        db: The MongoDB database.
    """

    client: MongoClient
    db: Database

    def __init__(self, database: str):
        """
        Initialize the MongoStorage class.
        Read credentials from env.
        """
        self.client = MongoClient(
            os.getenv("MONGO_URI"),
            username=os.getenv("MONGO_USER"),
            password=os.getenv("MONGO_PASSWORD"),
        )

        self.db = self.client[database]

    def find(self, table: str, query: dict, sort: List[str], asc: bool = True) -> List:
        """
        Find rows in a table that match the query. A query is a dictionary of key-equals-value pairs.
        """
        query = {key: {"$eq": value} for key, value in query.items()}
        sort = [(key, 1 if asc else -1) for key in sort]
        result = self.db[table].find(query)
        if sort:
            result = result.sort(sort)
        return list(result)

    def upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Upsert each item. For now, simply loop over them and insert each one separately.
        Also add an updated_at column.
        """
        updated_at = datetime.utcnow()
        table = self.db[table]
        for row in data:
            row["updated_at"] = updated_at
            table.update_one({key_col: row[key_col]}, {"$set": row}, upsert=True)

    def insert(self, data: List, table: str) -> None:
        """
        Insert all items in the data list.
        Also add an inserted_at column.
        """
        inserted_at = datetime.utcnow()
        data = map(lambda x: {**x, "inserted_at": inserted_at}, data)
        if data:
            table = self.db[table]
            table.insert_many(data)
