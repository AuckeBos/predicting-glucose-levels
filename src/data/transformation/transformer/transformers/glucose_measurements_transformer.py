from datetime import datetime
from typing import List

import pandas as pd

from src.data.table_metadata import TableMetadata
from src.data.transformation.transformer.transformers.abstract_transformer import (
    AbstractTransformer,
)


class GlucoseMeasurementTransformer(AbstractTransformer):
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
        Ingest the entries.
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
        df = pd.DataFrame(self.source)
        if df.empty:
            self.logger.info("No new entries found.")
            self.result = []
            return
        cols = {
            "glucose_measurement_id": df["_id"].astype(str),
            "glucose_measurement_time": pd.to_datetime(df["dateString"]),
            "delta": df["delta"].astype(float),
            "direction": df["direction"].astype(str),
            "glucose_value_mg_dl": df["sgv"].fillna(df["mbg"]).astype(float),
            "glucose_value_mmol_l": df["sgv"].fillna(df["mbg"]).astype(float) / 18.0182,
            "type": df["type"].astype(str),
            "updated_at": pd.to_datetime(datetime.now()),
        }
        # Select relevant cols, and cast and alias each
        self.result = df.assign(**cols)[cols.keys()]
        self.logger.info(f"Successfully transformed {len(self.result)} entries.")

    def load(self):
        if not self.result.empty:
            self.storage.upsert(
                self.result.to_dict("records"), self.destination_metadata.name
            )
        self.storage.set_last_runmoment(self.destination_metadata.name, self.runmoment)
