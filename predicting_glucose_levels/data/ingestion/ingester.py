from logging import Logger
from typing import List

from kink import inject
from prefect import flow, task
from prefect.logging.loggers import PrefectLogAdapter
from pydantic import validator

from predicting_glucose_levels.data.ingestion.loader.abstract_loader import (
    AbstractLoader,
)
from predicting_glucose_levels.data.storage.abstract_storage import AbstractStorage
from predicting_glucose_levels.data.table_metadata import TableMetadata


@inject
class Ingester:
    """
    The ingester class is used to ingest data from a data source into a storage.
    It uses a loader to load the data and a storage to store the data.

    Attributes:
        data_loader: The loader to use to load the data.
        storage: The storage to use to store the data.
        logger: The logger to use to log messages.
    """

    data_loader: AbstractLoader
    storage: AbstractStorage
    logger: PrefectLogAdapter
    tables: List[TableMetadata]

    def __init__(
        self,
        data_loader: AbstractLoader,
        storage: AbstractStorage,
        logger: PrefectLogAdapter,
    ):
        self.data_loader = data_loader
        self.storage = storage
        self.logger = logger

    def set_tables(self, tables: List[TableMetadata]):
        """
        Set the tables to ingest.
        """
        self.tables = tables

    def ingest(self, tables: List[TableMetadata]):
        """
        Ingest a new batch of data.
        Loop over all tables in the storage, and ingest the data.
        For each data type, read and write runmoments.
        """
        self.storage.setup()
        self.logger.info(f"Ingesting {len(tables)} tables")
        for table in tables:
            start, end = self.storage.get_window(table.name)
            self.logger.debug(f"Ingesting {table.name} from {start} to {end}")
            data = self.data_loader.load(
                start, end, table.endpoint, table.timestamp_col
            )
            self.storage.upsert(data, table.name)
            self.logger.debug(
                f"Ingested {len(data)} rows for {table.name} during window {start} to {end}"
            )
            self.storage.set_last_runmoment(table.name, end)
        self.logger.debug("Done ingesting")
