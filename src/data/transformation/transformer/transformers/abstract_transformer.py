from abc import ABC, abstractmethod

from kink import inject

from src.data.ingestion.ingester import Ingester
from src.data.transformation.transformer.validators.schema_validator import (
    SchemaValidator,
)


@inject
class AbstractTransformer(ABC):
    """
    Base Transformer class. Will be implemented by concrete transformer classes.
    Concrete implementations must implement ETL methods for specific destination tables.
    """

    schema_validator: SchemaValidator
    ingester: Ingester

    def __init__(self, ingester: Ingester, schema_validator: SchemaValidator):
        self.schema_validator = schema_validator
        self.ingester = ingester

    @abstractmethod
    def extract(self):
        """
        Extracts data from the source tables.
        Uses the ingester to extract the data.
        """
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self):
        """
        Validates the schema of the source tables.
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
        self.validate_schema()
        self.transform()
        self.load()
