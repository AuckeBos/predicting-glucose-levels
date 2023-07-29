from datetime import datetime

import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pytest import mark
from pytest_mock_resources import create_mongo_fixture
from src.data.storage.mongo_storage import MongoStorage

mongo = create_mongo_fixture()


@pytest.fixture
def mongo_storage(mongo):
    return MongoStorage(mongo, "test_database")


def test_insert_and_find_one(mongo_storage):
    # Arrange
    table_name = "test_table"
    data = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]

    # Act
    mongo_storage.insert(data, table_name)
    result = mongo_storage.find_one(table_name, {"key": 2})

    # Assert
    assert result == {"key": 2, "value": "two"}


def test_upsert_and_find(mongo_storage):
    # Arrange
    table_name = "test_table"
    data = [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]

    # Act
    mongo_storage.upsert(data, table_name, "key", "value")
    result = mongo_storage.find(table_name, {"key": {"$in": [1, 2]}}, ["key"])

    # Assert
    assert len(result) == 2
    assert {"key": 1, "value": "one"} in result
    assert {"key": 2, "value": "two"} in result


def test_get_window(mongo_storage):
    # Arrange
    source = "test_source"
    current_time = datetime.utcnow()

    # Act
    start, end = mongo_storage.get_window(source)

    # Assert
    assert start.year == 2022 and start.month == 1 and start.day == 1
    assert end <= current_time


def test_set_last_runmoment_and_get_last_runmoment(mongo_storage):
    # Arrange
    source = "test_source"
    timestamp = datetime(2023, 7, 29)

    # Act
    mongo_storage.set_last_runmoment(source, timestamp)
    result = mongo_storage.get_last_runmoment(source)

    # Assert
    assert result == timestamp
