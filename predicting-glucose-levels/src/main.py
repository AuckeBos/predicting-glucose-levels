import os
from src.data.ingestion.ingester import Ingester
from src.data.ingestion.loader.nightscout_loader import NightscoutLoader
from src.data.ingestion.storage.mongo_storage import MongoStorage
from src.helpers.general import load_env
import click

def initialize():
    load_env()


@click.group()
def cli():
    pass


@cli.command()
def ingest():
    loader = NightscoutLoader()
    db = os.getenv("MONGO_DB")
    storage = MongoStorage(db)
    ingester = Ingester(loader, storage)
    ingester.ingest()


if __name__ == "__main__":
    initialize()
    ingest()
    # cli()