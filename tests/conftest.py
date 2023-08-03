# conftest.py

import os
from pathlib import Path

import pytest
import requests
from kink import di

from src.data.metadata import Metadata
from src.helpers import config


@pytest.fixture(autouse=True)
def disable_network_calls(monkeypatch):
    """
    Disable network calls for every test.
    """

    def disable():
        raise RuntimeError("Network access not allowed during testing!")

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: disable())
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: disable())


@pytest.fixture(autouse=True)
def disable_network_calls(monkeypatch):
    """
    Set project dir to the test config dir for every test.
    """
    print("patching")
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    di[Metadata] = Metadata(current_dir / "config/metadata")
