import click
from kink import di, inject
from prefect import flow

from predicting_glucose_levels.data.ingestion.ingester import Ingester
from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.transformation.transformer.transformers.base_transformer import (
    BaseTransformer,
)
from predicting_glucose_levels.data.transformation.transformer.transformers.glucose_measurements_transformer import (
    GlucoseMeasurementTransformer,
)
from predicting_glucose_levels.helpers.config import PROJECT_DIR


@click.group
def cli():
    pass


@cli.command
@inject
@flow(validate_parameters=False, log_prints=True)
def ingest(metadata: Metadata):
    """
    Ingest all source tables.
    """
    print("test")
    ingester = Ingester()
    tables = [t for t in metadata.tables if t.type == "source_table"]
    ingester.ingest(tables)


@cli.command
@flow(validate_parameters=False, log_prints=True)
def transform():
    """
    Run all defined transformers.
    """
    transformers = [x() for x in BaseTransformer.__subclasses__()]
    for transformer in transformers:
        transformer.etl()


@cli.command
def test():
    """
    Testing function
    """
    print(BaseTransformer.__subclasses__())
    pass


@cli.command
def help():
    """
    Show the help message.
    """
    ctx = click.Context(cli)
    click.echo(ctx.get_help())


if __name__ == "__main__":
    pass
    ingest()
    # transform()
    # cli()
