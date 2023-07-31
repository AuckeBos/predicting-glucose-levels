import json
import os

import click
from jsonschema import validate
from pymongo import MongoClient

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.loader.nightscout_loader import NightscoutLoader
from src.data.ingestion.source_table import SourceTable
from src.data.storage.mongo_storage import MongoStorage
from src.helpers.general import PROJECT_DIR


@click.group()
def cli():
    pass


@cli.command()
def ingest():
    loader = NightscoutLoader()
    storage = MongoStorage()
    ingester = Ingester(loader, storage)
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
