from typing import List

from kink import inject
from pymongo.database import Database

from src.data.storage.abstract_storage import AbstractStorage


@inject
class MongoStorage(AbstractStorage):
    """
    The MongoStorage class is used to store data in a MongoDB database.

    Attributes:
        database: The MongoDB database.
    """

    database: Database

    def __init__(self, database: Database):
        """
        Create a connection to the MongoDB database.
        """
        self.database = database

    def find(
        self, table: str, query: dict, sort: List[str] = None, asc: bool = True
    ) -> List:
        """
        Find rows in a table that match the query. A query is a dictionary of key-equals-value pairs.
        """
        query = {key: {"$eq": value} for key, value in query.items()}
        sort = [(key, 1 if asc else -1) for key in sort]
        result = self.database[table].find(query)
        if sort:
            result = result.sort(sort)
        return list(result)

    def _upsert(self, data: List, table: str, key_col: str, timestamp_col: str) -> None:
        """
        Upsert each item. For now, simply loop over them and insert each one separately.
        Also add an updated_at column.
        """
        table = self.database[table]
        for row in data:
            table.update_one({key_col: row[key_col]}, {"$set": row}, upsert=True)

    def _insert(self, data: List, table: str) -> None:
        """
        Insert all items in the data list.
        Also add an inserted_at column.
        """
        if data:
            table = self.database[table]
            table.insert_many(data)
