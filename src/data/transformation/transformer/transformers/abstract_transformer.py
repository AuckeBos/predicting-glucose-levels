import json
from abc import ABC, abstractmethod
from logging import Logger
from typing import List

from kink import inject

from src.data.ingestion.ingester import Ingester
from src.data.metadata import Metadata
from src.data.storage.abstract_storage import AbstractStorage
from src.data.table_metadata import TableMetadata
from src.data.transformation.transformer.validators.schema_validator import (
    SchemaValidator,
)
from src.helpers.config import PROJECT_DIR


@inject
class AbstractTransformer(ABC):
    """
    Base Transformer class. Will be implemented by concrete transformer classes.
    Concrete implementations must implement ETL methods for specific destination tables.
    """

    schema_validator: SchemaValidator
    ingester: Ingester
    storage: AbstractStorage
    logger: Logger
    metadata: Metadata

    def __init__(
        self,
        storage: AbstractStorage,
        ingester: Ingester,
        schema_validator: SchemaValidator,
        logger: Logger,
        metadata: Metadata,
    ):
        self.storage = storage
        self.schema_validator = schema_validator
        self.ingester = ingester
        self.logger = logger
        self.metadata = metadata

    @abstractmethod
    def extract(self):
        """
        Extracts data from the source tables.
        Uses the ingester to extract the data.
        """
        raise NotImplementedError

    @abstractmethod
    def validate_schemas(self):
        """
        Validates the schema(s) of the source table(s).
        Uses the schema validator to validate the schema.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self):
        """
        Transforms the extracted data.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self):
        """
        Loads the transformed data into the destination tables.
        """
        raise NotImplementedError

    def etl(self):
        """
        Performs the ETL process.
        """
        self.extract()
        self.validate_schemas()
        self.transform()
        self.load()
