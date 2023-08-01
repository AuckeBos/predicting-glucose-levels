from typing import List

import pandas as pd

from src.data.transformation.transformer.transformers.abstract_transformer import (
    AbstractTransformer,
)


class GlucoseMeasurementTransformer(AbstractTransformer):
    source_name = "entries"
    source: List[dict]
    result: pd.DataFrame

    def validate_schemas(self):
        self.schema_validator.validate(self.source_name, self.source)

    def extract(self):
        """
        Ingest the entries.
        """
        source_table = self.get_source_table(self.source_name)
        self.ingester.ingest([source_table])
        self.source = self.storage.get(self.source_name)
        self.logger.info(f"Transforming {len(self.source)} entries.")

    def transform(self):
        df = pd.DataFrame(self.source)
        # Select relevant cols, and cast and alias each
        df = df.assign(
            **{
                "glucose_measurement_id": df["_id"].astype(str),
                "glucose_measurement_time": pd.to_datetime(df["dateString"]),
                "delta": df["delta"].astype(float),
                "direction": df["direction"].astype(str),
                "glucose_value_mg_dl": df["sgv"].fillna(df["mbg"]).astype(float),
                "glucose_value_mmol_l": df["sgv"].fillna(df["mbg"]).astype(float)
                / 18.0182,
            }
        )
        self.result = df
        self.logger.info(f"Successfully transformed {len(self.result)} entries.")

    def load(self):
        self.storage.overwrite(self.result, "glucose_measurements")
