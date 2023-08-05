"""
Initialize the dependency injection container and inject dependencies.
"""
import logging
import os

from dotenv import find_dotenv, load_dotenv
from kink import di
from prefect import get_run_logger
from prefect.logging.loggers import PrefectLogAdapter
from pymongo import MongoClient
from pymongo.database import Database

from predicting_glucose_levels.data.ingestion.ingester import Ingester
from predicting_glucose_levels.data.ingestion.loader.abstract_loader import (
    AbstractLoader,
)
from predicting_glucose_levels.data.ingestion.loader.nightscout_loader import (
    NightscoutLoader,
)
from predicting_glucose_levels.data.metadata import Metadata
from predicting_glucose_levels.data.storage.abstract_storage import AbstractStorage
from predicting_glucose_levels.data.storage.mongo_storage import MongoStorage
from predicting_glucose_levels.data.table_metadata import TableMetadata
from predicting_glucose_levels.helpers.config import LOGS_DIR, LOGS_FILE, METADATA_DIR


def _load_env():
    load_dotenv(find_dotenv())


def _get_logger(name: str):
    """
    Get a logger with a file handler.
    """
    logger = get_run_logger()
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=1000,
    )
    di[Database] = lambda _di: _di[MongoClient][os.getenv("MONGO_DB")]
    # Logging
    di[logging.Logger] = lambda di: _get_logger("logger")
    di[PrefectLogAdapter] = lambda di: get_run_logger()
    # Set the NightscoutLoader as the default loader.
    di[AbstractLoader] = lambda _di: NightscoutLoader(
        os.getenv("NIGHTSCOUT_URI"), os.getenv("NIGHTSCOUT_SECRET")
    )
    # Set the MongoStorage as the default storage
    di[AbstractStorage] = lambda _di: MongoStorage()
