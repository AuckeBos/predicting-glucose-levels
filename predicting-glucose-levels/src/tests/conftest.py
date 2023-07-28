# conftest.py

import pytest
import requests


@pytest.fixture(autouse=True)
def disable_network_calls(monkeypatch):
    """
    Disable network calls for every test.
    """

    def disable():
        raise RuntimeError("Network access not allowed during testing!")

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: disable())
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: disable())
