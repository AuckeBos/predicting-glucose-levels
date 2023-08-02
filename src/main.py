import json
from typing import List

import click
from kink import di, inject

from src.data.ingestion.ingester import Ingester
from src.data.metadata import Metadata
from src.data.transformation.transformer.transformers.glucose_measurements_transformer import (
    GlucoseMeasurementTransformer,
)
from src.helpers.config import PROJECT_DIR


@click.group()
def cli():
    pass


@cli.command()
@inject
def ingest(metadata: Metadata):
    ingester = Ingester()
    tables = [t for t in metadata.tables if t.type == "source_table"]
    ingester.ingest(tables)


@cli.command()
def transform():
    transformers = [GlucoseMeasurementTransformer()]
    for transformer in transformers:
        transformer.etl()


if __name__ == "__main__":
    # ingest()
    transform()
