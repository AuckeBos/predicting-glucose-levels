import logging
import os
from datetime import datetime
from pathlib import Path

import pytz
from dotenv import find_dotenv, load_dotenv

PROJECT_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / ".." / ".."
LOGS_DIR = PROJECT_DIR / "logs"
LOGS_FILE = LOGS_DIR / "logs.log"


def load_env():
    load_dotenv(find_dotenv())


def get_logger(name: str):
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logger = logging.getLogger(name)
    os.makedirs(LOGS_DIR, exist_ok=True)
    fhandler = logging.FileHandler(filename=LOGS_FILE, mode="a")
    formatter = logging.Formatter(log_fmt)
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)
    return logger


def now():
    return datetime.now(tz=pytz.timezone("Europe/Amsterdam"))
