from abc import ABC, abstractmethod
from logging import Logger

from kink import inject

from predicting_glucose_levels.data.ingestion.ingester import Ingester
from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.storage.abstract_storage import AbstractStorage
from predicting_glucose_levels.data.table_metadata import TableMetadata
from predicting_glucose_levels.data.transformation.transformer.validators.schema_validator import (
    SchemaValidator,
)
from predicting_glucose_levels.helpers.config import PROJECT_DIR


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
