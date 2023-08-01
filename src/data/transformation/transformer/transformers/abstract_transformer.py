import json
from abc import ABC, abstractmethod
from logging import Logger

from kink import inject

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.source_table import SourceTable
from src.data.storage.abstract_storage import AbstractStorage
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

    def __init__(
        self,
        storage: AbstractStorage,
        ingester: Ingester,
        schema_validator: SchemaValidator,
        logger: Logger,
    ):
        self.storage = storage
        self.schema_validator = schema_validator
        self.ingester = ingester
        self.logger = logger

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

    def get_source_table(self, name: str) -> SourceTable:
        """
        Gets the source table from the source tables config file.

        Args:
            name (str): The name of the source table.
        """
        source_path = PROJECT_DIR / "config" / "source_tables.json"
        source_table_dict = json.load(open(source_path))
        if name not in source_table_dict:
            raise ValueError(f"Source table {name} not found in source tables.")
        return SourceTable(**source_table_dict[name])

    def etl(self):
        """
        Performs the ETL process.
        """
        self.extract()
        self.validate_schemas()
        self.transform()
        self.load()
