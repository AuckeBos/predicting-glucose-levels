import logging
import os

from anyio import Path
from dotenv import find_dotenv, load_dotenv

PROJECT_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / ".." / ".." / ".."
LOGS_DIR = PROJECT_DIR / "logs"
LOGS_FILE = LOGS_DIR / "logs.log"


def load_env():
    load_dotenv(find_dotenv())


def get_logger(name: str):
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logger = logging.getLogger(name)
    fhandler = logging.FileHandler(filename=LOGS_FILE, mode="a")
    formatter = logging.Formatter(log_fmt)
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)
    return logger
