import json
import os

import click
from pymongo import MongoClient
from src.data.ingestion.ingester import Ingester
from src.data.ingestion.loader.nightscout_loader import NightscoutLoader
from src.data.ingestion.source_table import SourceTable
from src.data.storage.mongo_storage import MongoStorage
from src.helpers.general import PROJECT_DIR, get_logger, load_env


def initialize():
    load_env()


@click.group()
def cli():
    pass


@cli.command()
def ingest():
    loader = NightscoutLoader()
    db = os.getenv("MONGO_DB")
    client = MongoClient(
        os.getenv("MONGO_URI"),
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD"),
    )
    storage = MongoStorage(client, db)
    ingester = Ingester(loader, storage)
    source_tables = PROJECT_DIR / "config" / "source_tables.json"
    source_tables = [SourceTable(**i) for i in json.load(open(source_tables))]
    ingester.ingest(source_tables)


if __name__ == "__main__":
    initialize()
    ingest()
    # cli()
