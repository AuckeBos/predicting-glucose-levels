import click
from kink import di, inject
from prefect import flow, task
from prefect.deployments.deployments import Deployment

from predicting_glucose_levels.data import table_metadata
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


# @cli.command
# @inject
@flow(validate_parameters=False)
def ingest(metadata: Metadata):
    """
    Ingest all source tables.
    """

    @task
    def ingest_one(table: table_metadata):
        ingester = Ingester()
        ingester.ingest(tables=[table])

    tables = [t for t in metadata.tables if t.type == "source_table"]
    for table in tables:
        ingest_one(table)


@cli.command
@flow(validate_parameters=False)
def transform():
    """
    Run all defined transformers.
    """

    @task
    def transform_one(_cls):
        transformer = _cls()
        transformer.etl()

    for _cls in BaseTransformer.__subclasses__():
        transform_one(_cls)


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
