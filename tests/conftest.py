"""
Pytest fixtures for data engineering pipeline tests.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def sample_flight_raw_data():
    """Sample raw flight data from OpenSky API."""
    return {
        "time": 1609459200,
        "states": [
            [
                "abc123",
                "UAL123 ",
                "United States",
                1609459100,
                1609459200,
                -122.4,
                37.8,
                10000.0,
                False,
                250.0,
                90.0,
                5.0,
                None,
                10500.0,
                "1234",
                False,
                0,
            ],
            [
                "def456",
                "BAW456 ",
                "United Kingdom",
                1609459100,
                1609459200,
                -0.1,
                51.5,
                11000.0,
                False,
                280.0,
                180.0,
                -2.0,
                None,
                11500.0,
                "5678",
                False,
                0,
            ],
            [
                "ghi789",
                "DLH789 ",
                "Germany",
                1609459100,
                1609459200,
                8.5,
                50.0,
                9500.0,
                True,
                0.0,
                0.0,
                0.0,
                None,
                9800.0,
                "9012",
                False,
                0,
            ],
            [
                "jkl012",
                "AFR012 ",
                "France",
                1609459100,
                1609459200,
                2.3,
                48.9,
                12000.0,
                False,
                300.0,
                270.0,
                3.0,
                None,
                12500.0,
                "3456",
                False,
                0,
            ],
            [
                "mno345",
                "UAL999 ",
                "United States",
                1609459100,
                1609459200,
                -73.9,
                40.7,
                8000.0,
                False,
                220.0,
                45.0,
                10.0,
                None,
                8500.0,
                "7890",
                False,
                0,
            ],
        ],
    }


@pytest.fixture
def sample_flight_silver_df():
    """Sample silver-transformed flight data."""
    return pd.DataFrame(
        {
            "icao24": ["abc123", "def456", "ghi789", "jkl012", "mno345"],
            "origin_country": [
                "United States",
                "United Kingdom",
                "Germany",
                "France",
                "United States",
            ],
            "velocity": [250.0, 280.0, 0.0, 300.0, 220.0],
            "on_ground": [False, False, True, False, False],
        }
    )


@pytest.fixture
def sample_market_bronze_df():
    """Sample bronze market data from Alpha Vantage."""
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-08", "2024-01-15"],
            "1. open": ["150.00", "152.50", "155.00"],
            "2. high": ["153.00", "156.00", "158.00"],
            "3. low": ["148.00", "151.00", "153.50"],
            "4. close": ["152.00", "155.50", "157.00"],
            "5. volume": ["1000000", "1200000", "1100000"],
        }
    )


@pytest.fixture
def sample_market_silver_df():
    """Sample silver-transformed market data."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-08", "2024-01-15"]),
            "open": [150.0, 152.5, 155.0],
            "high": [153.0, 156.0, 158.0],
            "low": [148.0, 151.0, 153.5],
            "close": [152.0, 155.5, 157.0],
            "volume": [1000000, 1200000, 1100000],
            "day": [1, 8, 15],
            "month": [1, 1, 1],
            "year": [2024, 2024, 2024],
            "day_name": ["Monday", "Monday", "Monday"],
            "month_name": ["January", "January", "January"],
            "daily_return_pct": [1.33, 1.97, 1.29],
            "intraday_volatility_pct": [3.33, 2.30, 2.90],
        }
    )


@pytest.fixture
def mock_airflow_context():
    """Mock Airflow context for testing tasks."""
    ti = MagicMock()
    ti.xcom_pull = MagicMock(return_value=None)
    ti.xcom_push = MagicMock()

    return {
        "ti": ti,
        "ds": "2024-01-15",
        "ds_nodash": "20240115",
        "execution_date": "2024-01-15T00:00:00+00:00",
    }


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def bronze_flight_file(temp_data_dir, sample_flight_raw_data):
    """Create a temporary bronze flight data file."""
    bronze_path = temp_data_dir / "bronze_data"
    bronze_path.mkdir(parents=True, exist_ok=True)

    file_path = bronze_path / "flights_bronze_20240115.json"
    with open(file_path, "w") as f:
        json.dump(sample_flight_raw_data, f)

    return file_path


@pytest.fixture
def silver_flight_file(temp_data_dir, sample_flight_silver_df):
    """Create a temporary silver flight data file."""
    silver_path = temp_data_dir / "silver_data"
    silver_path.mkdir(parents=True, exist_ok=True)

    file_path = silver_path / "flights_silver_20240115.csv"
    sample_flight_silver_df.to_csv(file_path, index=False)

    return file_path


@pytest.fixture
def bronze_market_file(temp_data_dir, sample_market_bronze_df):
    """Create a temporary bronze market data file."""
    bronze_path = temp_data_dir / "bronze_data"
    bronze_path.mkdir(parents=True, exist_ok=True)

    file_path = bronze_path / "bronze_data_weekly_data.csv"
    sample_market_bronze_df.to_csv(file_path, index=False)

    return file_path


@pytest.fixture
def silver_market_file(temp_data_dir, sample_market_silver_df):
    """Create a temporary silver market data file."""
    silver_path = temp_data_dir / "silver_data"
    silver_path.mkdir(parents=True, exist_ok=True)

    file_path = silver_path / "silver_transformed_data.csv"
    sample_market_silver_df.to_csv(file_path, index=False)

    return file_path
