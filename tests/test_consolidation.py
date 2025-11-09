"""Tests for consolidation service and step."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3
import pandas as pd
from datetime import datetime

from ingestor_reader.domain.services.consolidation_service import (
    consolidate_month_projections,
    _read_events_for_month,
    _deduplicate_dataframe,
)
from ingestor_reader.use_cases.steps.consolidate_projection import (
    consolidate_projection_step,
    _extract_year_month,
    _write_series_projections,
)
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.domain.entities.dataset_config import (
    DatasetConfig,
    SourceConfig,
    ParseConfig,
    NormalizeConfig,
    OutputConfig,
)


@pytest.fixture
def aws_resources():
    """Create AWS resources (S3 bucket) for testing."""
    with mock_aws():
        # Create S3 bucket
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        yield {"s3_client": s3_client}


@pytest.fixture
def catalog(aws_resources):
    """Create S3Catalog instance for testing."""
    s3_storage = S3Storage(bucket="test-bucket", region="us-east-1")
    return S3Catalog(s3_storage)


@pytest.fixture
def dataset_config():
    """Create a test DatasetConfig."""
    return DatasetConfig(
        dataset_id="test_dataset",
        frequency="daily",
        lag_days=0,
        source=SourceConfig(kind="http", url="http://example.com/data.xlsx", format="xlsx"),
        parse=ParseConfig(plugin="test_parser"),
        normalize=NormalizeConfig(plugin="test_normalizer", primary_keys=["obs_time", "internal_series_code"]),
        output=OutputConfig(),
    )


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        "obs_time": pd.to_datetime([
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
        ]),
        "internal_series_code": ["SERIES_1", "SERIES_1", "SERIES_2"],
        "value": [1.0, 2.0, 3.0],
    })


def test_consolidate_month_projections_no_events(catalog):
    """Test consolidation when no events exist for the month."""
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    assert result == {}


def test_consolidate_month_projections_empty_events(catalog):
    """Test consolidation when events exist but are empty or invalid."""
    # Mock list_events_for_month to return some keys
    catalog.list_events_for_month = Mock(return_value=["event1.parquet", "event2.parquet"])
    
    # Mock get_object to raise exception (simulating invalid events)
    catalog.s3.get_object = Mock(side_effect=Exception("File not found"))
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    assert result == {}


def test_consolidate_month_projections_event_without_series_code(catalog, sample_data):
    """Test consolidation when an event doesn't have internal_series_code column."""
    # Mock list_events_for_month to return some keys
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    
    # Create data without internal_series_code
    data_without_series = sample_data.drop(columns=["internal_series_code"])
    
    # Mock reading events
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(return_value=data_without_series)
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should return empty dict because no valid events
    assert result == {}


def test_consolidate_month_projections_event_read_error(catalog):
    """Test consolidation when reading an event fails."""
    # Mock list_events_for_month to return some keys
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    
    # Mock get_object to raise exception
    catalog.s3.get_object = Mock(side_effect=Exception("Read error"))
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should return empty dict because no valid events
    assert result == {}


def test_consolidate_month_projections_success(catalog, sample_data):
    """Test successful consolidation with valid events."""
    # Mock list_events_for_month to return some keys
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    
    # Mock reading events
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(return_value=sample_data)
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should return dict with series projections
    assert len(result) == 2  # SERIES_1 and SERIES_2
    assert "SERIES_1" in result
    assert "SERIES_2" in result
    assert len(result["SERIES_1"]) == 2
    assert len(result["SERIES_2"]) == 1


def test_consolidate_month_projections_with_duplicates(catalog):
    """Test consolidation with duplicate rows (should deduplicate)."""
    # Create data with duplicates
    data_with_duplicates = pd.DataFrame({
        "obs_time": pd.to_datetime([
            "2024-01-01",
            "2024-01-01",  # Duplicate
            "2024-01-02",
        ]),
        "internal_series_code": ["SERIES_1", "SERIES_1", "SERIES_1"],
        "value": [1.0, 2.0, 3.0],
        "version": ["v1", "v2", "v1"],  # v2 should win
    })
    
    # Mock list_events_for_month
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    
    # Mock reading events
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(return_value=data_with_duplicates)
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should deduplicate (keep first = most recent version)
    assert len(result) == 1
    assert "SERIES_1" in result
    assert len(result["SERIES_1"]) == 2  # Duplicate removed


def test_deduplicate_dataframe_with_version(catalog):
    """Test deduplication when version column exists."""
    df = pd.DataFrame({
        "key": ["a", "a", "b"],
        "value": [1, 2, 3],
        "version": ["v1", "v2", "v1"],
    })
    
    result = _deduplicate_dataframe(df, ["key"])
    
    # Should keep most recent version (v2) for key "a"
    assert len(result) == 2
    assert result[result["key"] == "a"]["value"].iloc[0] == 2  # v2 wins


def test_deduplicate_dataframe_without_version(catalog):
    """Test deduplication when version column doesn't exist."""
    df = pd.DataFrame({
        "key": ["a", "a", "b"],
        "value": [1, 2, 3],
    })
    
    result = _deduplicate_dataframe(df, ["key"])
    
    # Should keep first occurrence
    assert len(result) == 2
    assert result[result["key"] == "a"]["value"].iloc[0] == 1


def test_consolidate_projection_step_empty_dataframe(catalog, dataset_config):
    """Test consolidation step with empty DataFrame."""
    empty_df = pd.DataFrame()
    
    # Should return early without error
    consolidate_projection_step(catalog, dataset_config, empty_df)


def test_consolidate_projection_step_no_date_column(catalog, dataset_config):
    """Test consolidation step when no date column is found."""
    df = pd.DataFrame({
        "internal_series_code": ["SERIES_1"],
        "value": [1.0],
    })
    
    # Should return early without error
    consolidate_projection_step(catalog, dataset_config, df)


def test_consolidate_projection_step_no_series_code(catalog, dataset_config):
    """Test consolidation step when no internal_series_code column is found."""
    df = pd.DataFrame({
        "obs_time": pd.to_datetime(["2024-01-01"]),
        "value": [1.0],
    })
    
    # Should return early without error
    consolidate_projection_step(catalog, dataset_config, df)


def test_consolidate_projection_step_write_error(catalog, dataset_config, sample_data):
    """Test consolidation step when writing projection fails."""
    # Mock list_events_for_month
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    
    # Mock reading events
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(return_value=sample_data)
    
    # Mock write_series_projection to raise exception
    catalog.write_series_projection = Mock(side_effect=Exception("Write error"))
    
    # Should not raise exception, just log error
    consolidate_projection_step(catalog, dataset_config, sample_data)


def test_consolidate_projection_step_consolidation_error(catalog, dataset_config, sample_data):
    """Test consolidation step when consolidation fails."""
    # Mock list_events_for_month
    catalog.list_events_for_month = Mock(side_effect=Exception("Consolidation error"))
    
    # Should not raise exception, just log error
    consolidate_projection_step(catalog, dataset_config, sample_data)


def test_extract_year_month_invalid_dates(catalog):
    """Test extracting year/month with invalid dates."""
    df = pd.DataFrame({
        "obs_time": ["invalid", "2024-01-01", None],
        "value": [1.0, 2.0, 3.0],
    })
    
    result = _extract_year_month(df, "obs_time")
    
    # Should filter out invalid dates
    assert len(result) == 1  # Only valid date remains
    assert result["year"].iloc[0] == 2024
    assert result["month"].iloc[0] == 1


def test_write_series_projections_success(catalog, sample_data):
    """Test writing series projections successfully."""
    # Group by series
    series_projections = {}
    for series_code, series_data in sample_data.groupby("internal_series_code"):
        series_projections[series_code] = series_data
    
    # Mock write_series_projection
    catalog.write_series_projection = Mock()
    
    _write_series_projections(catalog, "test_dataset", 2024, 1, series_projections)
    
    # Should write one projection per series
    assert catalog.write_series_projection.call_count == 2  # SERIES_1 and SERIES_2


def test_write_series_projections_partial_failure(catalog, sample_data):
    """Test writing series projections when one write fails."""
    # Group by series
    series_projections = {}
    for series_code, series_data in sample_data.groupby("internal_series_code"):
        series_projections[series_code] = series_data
    
    # Mock write_series_projection to fail for one series
    call_count = 0
    def mock_write(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Write error")
        return None
    
    catalog.write_series_projection = Mock(side_effect=mock_write)
    
    # Should not raise exception, just log error
    _write_series_projections(catalog, "test_dataset", 2024, 1, series_projections)
    
    # Should attempt to write both series
    assert catalog.write_series_projection.call_count == 2


def test_consolidate_month_projections_multiple_events(catalog):
    """Test consolidation with multiple events for the same month."""
    # Create two events with different data
    event1_data = pd.DataFrame({
        "obs_time": pd.to_datetime(["2024-01-01"]),
        "internal_series_code": ["SERIES_1"],
        "value": [1.0],
    })
    
    event2_data = pd.DataFrame({
        "obs_time": pd.to_datetime(["2024-01-02"]),
        "internal_series_code": ["SERIES_1"],
        "value": [2.0],
    })
    
    # Mock list_events_for_month
    catalog.list_events_for_month = Mock(return_value=["event1.parquet", "event2.parquet"])
    
    # Mock reading events (return different data for each event)
    read_count = 0
    def mock_read(*args, **kwargs):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return event1_data
        return event2_data
    
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(side_effect=mock_read)
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should consolidate both events
    assert len(result) == 1
    assert "SERIES_1" in result
    assert len(result["SERIES_1"]) == 2  # Both rows from both events


def test_consolidate_month_projections_mixed_valid_invalid_events(catalog, sample_data):
    """Test consolidation when some events are valid and some are invalid."""
    # Mock list_events_for_month
    catalog.list_events_for_month = Mock(return_value=["event1.parquet", "event2.parquet", "event3.parquet"])
    
    # Mock reading events: first valid, second invalid (no series_code), third valid
    read_count = 0
    def mock_read(*args, **kwargs):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return sample_data
        elif read_count == 2:
            # Event without internal_series_code
            return sample_data.drop(columns=["internal_series_code"])
        else:
            return sample_data
    
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(side_effect=mock_read)
    
    result = consolidate_month_projections(
        catalog, "test_dataset", 2024, 1, ["obs_time", "internal_series_code"]
    )
    
    # Should consolidate valid events (1 and 3), skip invalid (2)
    assert len(result) == 2  # SERIES_1 and SERIES_2
    assert "SERIES_1" in result
    assert "SERIES_2" in result

