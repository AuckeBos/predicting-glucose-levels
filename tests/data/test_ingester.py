from unittest.mock import Mock

import pytest

from src.data.ingestion.ingester import Ingester
from src.data.ingestion.loader.abstract_loader import AbstractLoader
from src.data.storage.abstract_storage import AbstractStorage
from src.data.table_metadata import TableMetadata


@pytest.fixture
def mock_data_loader():
    return Mock(spec=AbstractLoader)


@pytest.fixture
def mock_storage():
    return Mock(spec=AbstractStorage)


@pytest.fixture
def ingester(mock_data_loader, mock_storage):
    return Ingester(mock_data_loader, mock_storage)


def test_ingest_single_table(ingester, mock_data_loader, mock_storage):
    # Arrange
    table = TableMetadata(
        name="table_1",
        endpoint="endpoint",
        timestamp_col="timestamp",
        key_col="key",
        type="source_table",
    )
    tables = [table]
    mock_storage.get_window.side_effect = [("2023-07-28", "2023-07-29")]
    mock_data_loader.load.return_value = [
        {"key": 1, "timestamp": "2023-07-28T12:00:00"},
        {"key": 2, "timestamp": "2023-07-29T08:30:00"},
    ]

    # Act
    ingester.ingest(tables)

    # Assert
    mock_storage.get_window.assert_called_with("table_1")
    mock_data_loader.load.assert_called_with(
        "2023-07-28", "2023-07-29", "endpoint", "timestamp"
    )
    mock_storage.upsert.assert_called_with(
        [
            {"key": 1, "timestamp": "2023-07-28T12:00:00"},
            {"key": 2, "timestamp": "2023-07-29T08:30:00"},
        ],
        "table_1",
    )
    mock_storage.set_last_runmoment.assert_called_with("table_1", "2023-07-29")


def test_ingest_multiple_tables(ingester, mock_data_loader, mock_storage):
    # Arrange
    table1 = TableMetadata(
        name="table_1",
        endpoint="endpoint1",
        timestamp_col="timestamp",
        key_col="key",
        type="source_table",
    )
    table2 = TableMetadata(
        name="table_2",
        endpoint="endpoint2",
        timestamp_col="timestamp",
        key_col="key",
        type="source_table",
    )
    tables = [table1, table2]
    mock_storage.get_window.side_effect = [
        ("2023-07-28", "2023-07-29"),
        ("2023-07-27", "2023-07-28"),
    ]
    mock_data_loader.load.side_effect = [
        [
            {"key": 1, "timestamp": "2023-07-28T12:00:00"},
            {"key": 2, "timestamp": "2023-07-29T08:30:00"},
        ],
        [
            {"key": 3, "timestamp": "2023-07-27T18:00:00"},
            {"key": 4, "timestamp": "2023-07-28T06:30:00"},
        ],
    ]

    # Act
    ingester.ingest(tables)

    # Assert
    assert mock_storage.get_window.call_count == 2
    assert mock_data_loader.load.call_count == 2
    assert mock_storage.upsert.call_count == 2
    assert mock_storage.set_last_runmoment.call_count == 2
