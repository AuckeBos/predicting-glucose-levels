import click
from kink import di, inject

from predicting_glucose_levels.data.ingestion.ingester import Ingester
from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.transformation.transformer.transformers.glucose_measurements_transformer import (
    GlucoseMeasurementTransformer,
)
from predicting_glucose_levels.helpers.config import PROJECT_DIR


@click.group
def cli():
    pass


@cli.command
@inject
def ingest(metadata: Metadata):
    """
    Ingest all source tables.
    """
    ingester = Ingester()
    tables = [t for t in metadata.tables if t.type == "source_table"]
    ingester.ingest(tables)


@cli.command
def transform():
    transformers = [GlucoseMeasurementTransformer()]
    for transformer in transformers:
        transformer.etl()


@cli.command
def test():
    """
    Testing function
    """
    pass


if __name__ == "__main__":
    pass
    ingest()
    # transform()
    # cli()
