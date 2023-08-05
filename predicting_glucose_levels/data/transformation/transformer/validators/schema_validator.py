import jsonschema
from kink import inject

from predicting_glucose_levels.data.metadata import Metadata


@inject
class SchemaValidator:
    """
    Implements a validate() function, that consumes a JSON document and validates it against a JSON schema.

    Attributes:
        metadata: The metadata class. Used to load the JSON schema.
    """

    metadata: Metadata

    def __init__(self, metadata: Metadata) -> None:
        self.metadata = metadata

    def validate(self, table_name: str, data: dict) -> None:
        """
        Validate a JSON document against a JSON schema.

        Args:
            table_name (str): The name of the table to validate against.
            data (dict): The JSON document to validate.

        """
        schema = self.metadata.get_table(table_name).schema
        jsonschema.validate(data, schema)
