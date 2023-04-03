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
        Ingest a new batch of data. Load the window of the storage, upsert all measurements, update
        the last runmoment.
        """
        start, end = self.storage.get_window()
        data = self.data_loader.load(start, end)
        self.storage.upsert_measurements(data)
        self.storage.set_last_runmoment(end)
