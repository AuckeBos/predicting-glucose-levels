"""
Define the configuration variables for the project.
"""
import os
from pathlib import Path

from kink import di

PROJECT_DIR = di["project_dir"]
CONFIG_DIR = PROJECT_DIR / "config"
METADATA_DIR = CONFIG_DIR / "metadata"
LOGS_DIR = PROJECT_DIR / "logs"
LOGS_FILE = LOGS_DIR / "logs.log"
