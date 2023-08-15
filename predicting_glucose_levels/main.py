import click
from kink import di, inject
from prefect import flow, task
from prefect.deployments.deployments import Deployment

from predicting_glucose_levels.data import table_metadata
from predicting_glucose_levels.data.ingestion.ingester import Ingester
from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.transformation.transformer.transformers import *
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

    tables = [t for t in metadata.tables if t.type == "source_table"]
    ingester = Ingester()
    ingester.ingest(tables=tables)


@cli.command
def transform():
    """
    Run all defined transformers.
    """
    transformers = [GlucoseMeasurementTransformer]
    for transformer in transformers:
        transformer.etl()


@cli.command
def test():
    """
    Testing function
    """
    print(BaseTransformer.__subclasses__())


@cli.command
def help():
    """
    Show the help message.
    """
    ctx = click.Context(cli)
    click.echo(ctx.get_help())


if __name__ == "__main__":
    pass
    # ingest()
    # transform()
    # cli()
