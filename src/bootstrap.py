import logging
import os

from dotenv import find_dotenv, load_dotenv
from kink import di
from pymongo import MongoClient
from pymongo.database import Database

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.loader.abstract_loader import AbstractLoader
from src.data.ingestion.loader.nightscout_loader import NightscoutLoader
from src.data.metadata import Metadata
from src.data.storage.abstract_storage import AbstractStorage
from src.data.storage.mongo_storage import MongoStorage
from src.data.table_metadata import TableMetadata
from src.helpers.config import LOGS_DIR, LOGS_FILE, METADATA_DIR


def _load_env():
    load_dotenv(find_dotenv())


def _get_logger(name: str):
    """
    Get a logger with a file handler.
    """
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logger = logging.getLogger(name)
    os.makedirs(LOGS_DIR, exist_ok=True)
    fhandler = logging.FileHandler(filename=LOGS_FILE, mode="a")
    formatter = logging.Formatter(log_fmt)
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)
    return logger


def bootstrap_di():
    """
    Inject dependencies into the dependency injection container.
    """
    _load_env()
    # Mongo
    di[MongoClient] = lambda _di: MongoClient(
        os.getenv("MONGO_URI"),
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD"),
    )
    di[Database] = lambda _di: _di[MongoClient][os.getenv("MONGO_DB")]
    # Logging
    di[logging.Logger] = _get_logger("logger")
    # Nightscout
    di["nightscout_uri"] = os.getenv("NIGHTSCOUT_URI")
    di["nightscout_secret"] = os.getenv("NIGHTSCOUT_SECRET")
    # Ingesti
    di[AbstractLoader] = lambda _di: NightscoutLoader()
    # Storage
    di[AbstractStorage] = lambda _di: MongoStorage()
