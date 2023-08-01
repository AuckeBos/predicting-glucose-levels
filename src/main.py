import json

import click

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.source_table import SourceTable
from src.data.transformation.transformer.transformers.glucose_measurements_transformer import (
    GlucoseMeasurementTransformer,
)
from src.helpers.config import PROJECT_DIR


@click.group()
def cli():
    pass


@cli.command()
def ingest():
    ingester = Ingester()
    source_tables = PROJECT_DIR / "config" / "source_tables.json"
    source_tables = [
        SourceTable(**dct) for dct in json.load(open(source_tables)).values()
    ]
    ingester.ingest(source_tables)


@cli.command()
def transform():
    transformers = [GlucoseMeasurementTransformer()]
    for transformer in transformers:
        transformer.etl()


if __name__ == "__main__":
    # ingest()
    transform()
    # cli()
