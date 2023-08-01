import datetime
import json

import click

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.source_table import SourceTable
from src.helpers.general import PROJECT_DIR


@click.group()
def cli():
    pass


@cli.command()
def ingest():
    ingester = Ingester()
    source_tables = PROJECT_DIR / "config" / "source_tables.json"
    source_tables = [SourceTable(**i) for i in json.load(open(source_tables))]
    ingester.ingest(source_tables)


@cli.command()
def transform():
    pass


if __name__ == "__main__":
    ingest()
    # transform()
    # cli()
