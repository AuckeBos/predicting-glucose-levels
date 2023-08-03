from datetime import datetime
from unittest.mock import Mock

import mongomock
import pytest
from kink import di

from src.data.metadata import Metadata
from src.data.storage.mongo_storage import MongoStorage
from src.data.table_metadata import TableMetadata
from src.helpers import config


@pytest.fixture
def mongo():
    return mongomock.MongoClient()


@pytest.fixture
def metadata():
    return Mock(spec=Metadata)


@pytest.fixture
def mongo_storage(mongo, metadata):
    return MongoStorage(mongo, mongo["test_database"], metadata)


def test_insert_and_find_one(mongo_storage):
    """
    Test that inserting data into a table and then finding it works.
    Also test that the inserted_at column is added.
    """
    # Arrange
    current_time = datetime.utcnow()
    table_name = "test_table"
    data = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]

    # Act
    mongo_storage.insert(data, table_name)
    result = mongo_storage.find_one(table_name, [("key", "eq", 2)])

    # Assert
    assert result["key"] == 2
    assert (
        datetime.fromisoformat(result["inserted_at"]).replace(tzinfo=None)
        > current_time
    )


def test_upsert_and_find(mongo_storage):
    # Arrange
    table_name = "test_table"
    data = [
        {"key": 1, "value": "one"},
        {"key": 2, "value": "two"},
    ]
    mongo_storage.metadata.get_table.return_value = TableMetadata(
        name="test_table", key_col="key", timestamp_col="timestamp", type="test_table"
    )

    # Act
    mongo_storage.upsert(data, table_name)
    result = mongo_storage.find(table_name, [("key", "in", [1, 2])], ["key"], False)

    # Assert
    assert len(result) == 2
    assert result[0]["key"] == 2
    assert result[1]["key"] == 1


def test_get_window(mongo_storage):
    """
    Test that the window is set and retrieved correctly.
    """
    # Arrange
    source = "test_source"
    current_time = datetime.utcnow().replace(microsecond=0)

    mongo_storage.metadata.get_table().key_col = "source"

    # Act
    mongo_storage.set_last_runmoment(source, current_time)
    start, end = mongo_storage.get_window(source)

    # Assert
    assert start == current_time
    assert end > current_time
