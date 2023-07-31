from datetime import datetime

import mongomock
import pytest

from src.data.storage.mongo_storage import MongoStorage


@pytest.fixture
def mongo():
    return mongomock.MongoClient()


@pytest.fixture
def mongo_storage(mongo):
    return MongoStorage(mongo, "test_database")


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
    result = mongo_storage.find_one(table_name, {"key": 2})

    # Assert
    assert result["key"] == 2
    assert result["inserted_at"] > current_time


def test_upsert_and_find(mongo_storage):
    # Arrange
    table_name = "test_table"
    data = [
        {"key": 1, "value": "one", "class": "test"},
        {"key": 2, "value": "two", "class": "test"},
    ]

    # Act
    mongo_storage.upsert(data, table_name, "key", "value")
    result = mongo_storage.find(table_name, {"class": "test"}, ["key"], False)

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

    # Act
    mongo_storage.set_last_runmoment(source, current_time)
    start, end = mongo_storage.get_window(source)

    # Assert
    assert start == current_time
    assert end > current_time
