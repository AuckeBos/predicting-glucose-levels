from pymongo import MongoClient
import os

from src.data.ingestion.loader.abstract_loader import AbstractLoader
from src.data.ingestion.storage.abstract_storage import AbstractStorage


class Ingester:
    """
    The ingester class is used to ingest data from a data source into a storage.
    It uses a loader to load the data and a storage to store the data.

    Attributes:
        data_loader: The loader to use to load the data.
        storage: The storage to use to store the data.
    """

    data_loader: AbstractLoader
    storage: AbstractStorage

    def __init__(self, data_loader: AbstractLoader, storage: AbstractStorage):
        self.data_loader = data_loader
        self.storage = storage

    def ingest(self):
        """
        Ingest a new batch of data.
        Loop over all tables in the storage, and ingest the data.
        For each data type, read and write runmoments.
        """
        for table in self.storage.TABLES:
            start, end = self.storage.get_window(table)
            data = self.data_loader.load(start, end, table)
            self.storage.upsert(data, table)
            self.storage.set_last_runmoment(table, end)
