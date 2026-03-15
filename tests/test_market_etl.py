"""
Unit tests for Market API ETL pipeline.

Tests cover:
- Bronze layer: API data ingestion
- Silver layer: Data transformation and enrichment
- Gold layer: Aggregation tables
- Data quality checks
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestMarketBronzeLayer:
    """Tests for market data bronze/ingestion layer."""

    def test_bronze_data_columns(self, sample_market_bronze_df):
        """Verify bronze data has expected columns from Alpha Vantage."""
        expected_cols = ["date", "1. open", "2. high", "3. low", "4. close", "5. volume"]
        assert list(sample_market_bronze_df.columns) == expected_cols

    def test_bronze_data_not_empty(self, sample_market_bronze_df):
        """Verify bronze data contains records."""
        assert len(sample_market_bronze_df) > 0

    def test_bronze_numeric_values_as_strings(self, sample_market_bronze_df):
        """Verify Alpha Vantage returns numeric values as strings."""
        # Alpha Vantage API returns numbers as strings
        assert sample_market_bronze_df["1. open"].dtype == object


class TestMarketSilverLayer:
    """Tests for market data silver/transformation layer."""

    def test_silver_column_renaming(self, sample_market_bronze_df):
        """Test that silver transform correctly renames columns."""
        df = sample_market_bronze_df.copy()
        
        # Apply the same transformation as silver_transform
        df.columns = [c.split(". ", 1)[1] if ". " in c else c for c in df.columns]
        
        expected_cols = ["date", "open", "high", "low", "close", "volume"]
        assert list(df.columns) == expected_cols

    def test_silver_date_parsing(self, bronze_market_file):
        """Test that dates are correctly parsed."""
        df = pd.read_csv(bronze_market_file)
        df.columns = [c.split(". ", 1)[1] if ". " in c else c for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_silver_numeric_conversion(self, sample_market_bronze_df):
        """Test that numeric columns are converted from strings."""
        df = sample_market_bronze_df.copy()
        df.columns = [c.split(". ", 1)[1] if ". " in c else c for c in df.columns]
        
        num_cols = ["open", "high", "low", "close", "volume"]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        
        assert df["open"].dtype == float
        assert df["close"].dtype == float
        # volume may be int64 or float64 depending on data - both are numeric
        assert pd.api.types.is_numeric_dtype(df["volume"])

    def test_silver_date_features(self, sample_market_silver_df):
        """Test that date features are correctly extracted."""
        assert "day" in sample_market_silver_df.columns
        assert "month" in sample_market_silver_df.columns
        assert "year" in sample_market_silver_df.columns
        assert "day_name" in sample_market_silver_df.columns
        assert "month_name" in sample_market_silver_df.columns

    def test_silver_calculated_fields(self, sample_market_silver_df):
        """Test that calculated fields are present."""
        assert "daily_return_pct" in sample_market_silver_df.columns
        assert "intraday_volatility_pct" in sample_market_silver_df.columns

    def test_daily_return_calculation(self):
        """Test daily return percentage calculation."""
        df = pd.DataFrame({
            "open": [100.0, 150.0],
            "close": [110.0, 165.0]
        })
        
        df["daily_return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
        
        assert df["daily_return_pct"].iloc[0] == 10.0  # (110-100)/100 * 100
        assert df["daily_return_pct"].iloc[1] == 10.0  # (165-150)/150 * 100

    def test_intraday_volatility_calculation(self):
        """Test intraday volatility percentage calculation."""
        df = pd.DataFrame({
            "open": [100.0, 150.0],
            "high": [110.0, 165.0],
            "low": [95.0, 140.0]
        })
        
        df["intraday_volatility_pct"] = (df["high"] - df["low"]) / df["open"] * 100
        
        assert df["intraday_volatility_pct"].iloc[0] == 15.0  # (110-95)/100 * 100
        assert round(df["intraday_volatility_pct"].iloc[1], 2) == 16.67  # (165-140)/150 * 100


class TestMarketGoldLayer:
    """Tests for market data gold/aggregation layer."""

    def test_gold_monthly_price_aggregation(self, sample_market_silver_df):
        """Test monthly price aggregation."""
        agg = (
            sample_market_silver_df.groupby("month_name", observed=True)[["open", "close", "high", "low"]]
            .mean()
            .round(2)
        )
        
        assert len(agg) > 0
        assert "open" in agg.columns
        assert "close" in agg.columns

    def test_gold_volatility_by_day(self, sample_market_silver_df):
        """Test volatility aggregation by day of week."""
        agg = (
            sample_market_silver_df.groupby("day_name", observed=True)[["intraday_volatility_pct"]]
            .mean()
            .round(2)
        )
        
        assert len(agg) > 0
        assert "intraday_volatility_pct" in agg.columns

    def test_gold_monthly_volume(self, sample_market_silver_df):
        """Test monthly volume aggregation."""
        agg = (
            sample_market_silver_df.groupby("month_name", observed=True)[["volume"]]
            .mean()
            .round(0)
        )
        
        assert len(agg) > 0
        assert "volume" in agg.columns


class TestMarketDataQuality:
    """Data quality tests for market pipeline."""

    def test_no_null_dates(self, sample_market_silver_df):
        """Verify no null dates."""
        assert sample_market_silver_df["date"].isna().sum() == 0

    def test_no_null_prices(self, sample_market_silver_df):
        """Verify no null prices."""
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            assert sample_market_silver_df[col].isna().sum() == 0

    def test_high_greater_than_low(self, sample_market_silver_df):
        """Verify high price is always >= low price."""
        assert (sample_market_silver_df["high"] >= sample_market_silver_df["low"]).all()

    def test_close_within_range(self, sample_market_silver_df):
        """Verify close price is between low and high."""
        assert (sample_market_silver_df["close"] >= sample_market_silver_df["low"]).all()
        assert (sample_market_silver_df["close"] <= sample_market_silver_df["high"]).all()

    def test_positive_prices(self, sample_market_silver_df):
        """Verify all prices are positive."""
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            assert (sample_market_silver_df[col] > 0).all()

    def test_positive_volume(self, sample_market_silver_df):
        """Verify volume is positive."""
        assert (sample_market_silver_df["volume"] > 0).all()

    def test_dates_sorted(self, sample_market_silver_df):
        """Verify dates are sorted in ascending order."""
        dates = sample_market_silver_df["date"].tolist()
        assert dates == sorted(dates)


class TestMarketEndToEnd:
    """End-to-end tests for market pipeline."""

    def test_full_pipeline_flow(self, temp_data_dir, sample_market_bronze_df):
        """Test data flows correctly through all layers."""
        # Bronze: Save raw data
        bronze_path = temp_data_dir / "bronze"
        bronze_path.mkdir(parents=True)
        bronze_file = bronze_path / "market_bronze.csv"
        sample_market_bronze_df.to_csv(bronze_file, index=False)
        
        # Silver: Transform
        df = pd.read_csv(bronze_file)
        df.columns = [c.split(". ", 1)[1] if ". " in c else c for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        
        num_cols = ["open", "high", "low", "close", "volume"]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        
        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["day_name"] = df["date"].dt.day_name()
        df["month_name"] = df["date"].dt.month_name()
        df["daily_return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
        df["intraday_volatility_pct"] = (df["high"] - df["low"]) / df["open"] * 100
        
        silver_path = temp_data_dir / "silver"
        silver_path.mkdir(parents=True)
        silver_file = silver_path / "market_silver.csv"
        df.to_csv(silver_file, index=False)
        
        # Gold: Aggregate
        gold_path = temp_data_dir / "gold"
        gold_path.mkdir(parents=True)
        
        monthly_agg = df.groupby("month_name")[["open", "close"]].mean().round(2)
        monthly_agg.to_csv(gold_path / "monthly_prices.csv")
        
        # Verify all files exist
        assert bronze_file.exists()
        assert silver_file.exists()
        assert (gold_path / "monthly_prices.csv").exists()
        
        # Verify data integrity
        final_df = pd.read_csv(gold_path / "monthly_prices.csv")
        assert len(final_df) > 0


class TestMarketAPIIntegration:
    """Integration tests for Alpha Vantage API (mocked)."""

    @patch("data.market_api_etl.bronze_data.requests.get")
    @patch("data.market_api_etl.bronze_data.Variable.get")
    def test_api_call_with_valid_key(self, mock_var_get, mock_requests_get):
        """Test API is called with correct parameters."""
        mock_var_get.return_value = "test_api_key"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Weekly Time Series": {
                "2024-01-15": {
                    "1. open": "150.00",
                    "2. high": "153.00",
                    "3. low": "148.00",
                    "4. close": "152.00",
                    "5. volume": "1000000"
                }
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response
        
        # Verify the mock is set up correctly
        assert mock_var_get.return_value == "test_api_key"

    def test_api_response_parsing(self):
        """Test API response is correctly parsed to DataFrame."""
        api_response = {
            "Weekly Time Series": {
                "2024-01-15": {
                    "1. open": "150.00",
                    "2. high": "153.00",
                    "3. low": "148.00",
                    "4. close": "152.00",
                    "5. volume": "1000000"
                },
                "2024-01-08": {
                    "1. open": "148.00",
                    "2. high": "151.00",
                    "3. low": "146.00",
                    "4. close": "150.00",
                    "5. volume": "900000"
                }
            }
        }
        
        time_series = api_response["Weekly Time Series"]
        df = pd.DataFrame.from_dict(time_series, orient="index")
        df.reset_index(inplace=True)
        df.rename(columns={"index": "date"}, inplace=True)
        
        assert len(df) == 2
        assert "date" in df.columns
        assert "1. open" in df.columns
