from logging import Logger
from typing import List
from pymongo import MongoClient
import os

from src.data.ingestion.loader.abstract_loader import AbstractLoader
from src.data.storage.abstract_storage import AbstractStorage
from src.data.ingestion.source_table import SourceTable
from src.helpers.general import get_logger


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
    logger: Logger

    def __init__(self, data_loader: AbstractLoader, storage: AbstractStorage):
        self.data_loader = data_loader
        self.storage = storage
        self.logger = get_logger("Ingester")

    def ingest(self, tables: List[SourceTable]):
        """
        Ingest a new batch of data.
        Loop over all tables in the storage, and ingest the data.
        For each data type, read and write runmoments.
        """
        self.logger.info(f"Ingesting {len(tables)} tables")
        for table in tables:
            start, end = self.storage.get_window(table.destination_name)
            data = self.data_loader.load(
                start, end, table.endpoint, table.timestamp_col
            )
            self.storage.upsert(
                data, table.destination_name, table.key_col, table.timestamp_col
            )
            self.logger.info(
                f"Ingested {len(data)} rows for {table.destination_name} during window {start} to {end}"
            )
            self.storage.set_last_runmoment(table.destination_name, end)
            self.logger.info(
                f"Updated end timestamp for {table.destination_name} to {end}"
            )
        self.logger.info("Done ingesting")
