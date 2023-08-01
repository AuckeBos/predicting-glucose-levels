from typing import Dict

import jsonschema
from kink import inject


@inject
class SchemaValidator:
    """
    Implements a validate() function, that consumes a JSON document and validates it against a JSON schema.
    """

    schemas: Dict[str, dict]

    def __init__(self, schemas: Dict[str, dict]):
        self.schemas = schemas

    def validate(self, table_name: str, data: dict) -> None:
        """
        Validate a JSON document against a JSON schema.

        Args:
            table_name (str): The name of the table to validate against.
            data (dict): The JSON document to validate.

        """
        if table_name not in self.schemas:
            raise ValueError(f"Table {table_name} not found in schemas.")
        jsonschema.validate(data, self.schemas[table_name])
