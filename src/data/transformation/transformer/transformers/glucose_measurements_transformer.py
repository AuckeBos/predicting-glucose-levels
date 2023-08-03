from datetime import datetime
from typing import List

import pandas as pd

from src.data.table_metadata import TableMetadata
from src.data.transformation.transformer.transformers.abstract_transformer import (
    AbstractTransformer,
)


class GlucoseMeasurementTransformer(AbstractTransformer):
    """
    The GlucoseMeasurementTransformer class is used to transform the entries into glucose measurements.

    Attributes:
        source_metadata: The metadata of the source table.
        destination_metadata: The metadata of the destination table.
        source: The source data.
        result: The result of the transformation.
        runmoment: The start of the transformation. The runmoment of the destination table will be set to this value.
    """

    source_metadata: TableMetadata
    destination_metadata: TableMetadata
    source: List[dict]
    result: pd.DataFrame
    runmoment: datetime

    def __init__(self):
        super().__init__()
        self.source_metadata = self.metadata.get_table("entries")
        self.destination_metadata = self.metadata.get_table("glucose_measurements")
        self.runmoment = datetime.now()

    def validate_schemas(self):
        self.schema_validator.validate(self.source_metadata.name, self.source)

    def extract(self):
        """
        Ingest new entries. Then load only the new entries since the last runmoment of the destination table.
        """
        self.ingester.ingest([self.source_metadata])
        last_runmoment = self.storage.get_last_runmoment(self.destination_metadata.name)
        self.source = self.storage.find(
            self.source_metadata.name,
            [
                ("type", "ne", "cal"),
                (self.source_metadata.timestamp_col, "gt", last_runmoment.isoformat()),
            ],
        )
        self.logger.info(f"Transforming {len(self.source)} entries.")

    def transform(self):
        """
        Transform the entries into glucose measurements. Include conversion from mg/dL to mmol/L.
        """
        df = pd.DataFrame(self.source)
        if df.empty:
            self.logger.info("No new entries found.")
            self.result = None
            return
        cols = {
            "glucose_measurement_id": df["_id"].astype(str),
            "glucose_measurement_time": pd.to_datetime(df["dateString"]),
            "delta": df["delta"].astype(float),
            "direction": df["direction"].astype(str),
            "glucose_value_mg_dl": df["sgv"]
            .fillna(df.get("mbg", pd.Series()))
            .astype(float),
            "type": df["type"].astype(str),
            "updated_at": pd.to_datetime(datetime.now()),
        }
        # Select relevant cols, and cast and alias each
        df = df.assign(**cols)[cols.keys()]
        df[["glucose_value_mmol_l"]] = df[["glucose_value_mg_dl"]] / 18.0182
        self.result = df
        self.logger.info(f"Successfully transformed {len(self.result)} entries.")

    def load(self):
        """
        Upsert the transformed entries into the destination table. Then update the last runmoment of the destination table.
        """
        if self.result is not None:
            self.storage.upsert(
                self.result.to_dict("records"), self.destination_metadata.name
            )
        self.storage.set_last_runmoment(self.destination_metadata.name, self.runmoment)
