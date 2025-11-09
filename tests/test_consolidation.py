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
    _get_affected_months,
    _is_already_consolidated,
    _consolidate_month,
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
    """Test writing series projections successfully using WAL pattern."""
    # Group by series
    series_projections = {}
    for series_code, series_data in sample_data.groupby("internal_series_code"):
        series_projections[series_code] = series_data
    
    # Mock WAL methods
    catalog.write_series_projection_temp = Mock()
    catalog.move_series_projection_from_temp = Mock()
    
    _write_series_projections(catalog, "test_dataset", 2024, 1, series_projections)
    
    # Should write to temp and move for each series
    assert catalog.write_series_projection_temp.call_count == 2  # SERIES_1 and SERIES_2
    assert catalog.move_series_projection_from_temp.call_count == 2


def test_write_series_projections_temp_write_failure(catalog, sample_data):
    """Test writing series projections when temp write fails."""
    # Group by series
    series_projections = {}
    for series_code, series_data in sample_data.groupby("internal_series_code"):
        series_projections[series_code] = series_data
    
    # Mock write_series_projection_temp to fail
    catalog.write_series_projection_temp = Mock(side_effect=Exception("Temp write error"))
    
    # Should raise exception
    with pytest.raises(Exception, match="Temp write error"):
        _write_series_projections(catalog, "test_dataset", 2024, 1, series_projections)


def test_write_series_projections_move_failure(catalog, sample_data):
    """Test writing series projections when move fails."""
    # Group by series
    series_projections = {}
    for series_code, series_data in sample_data.groupby("internal_series_code"):
        series_projections[series_code] = series_data
    
    # Mock temp write succeeds, move fails
    catalog.write_series_projection_temp = Mock()
    catalog.move_series_projection_from_temp = Mock(side_effect=Exception("Move error"))
    
    # Should raise exception
    with pytest.raises(Exception, match="Move error"):
        _write_series_projections(catalog, "test_dataset", 2024, 1, series_projections)


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


def test_get_affected_months(catalog):
    """Test extracting affected months from DataFrame."""
    df = pd.DataFrame({
        "year": [2024, 2024, 2025],
        "month": [1, 2, 1],
        "value": [1.0, 2.0, 3.0],
    })
    
    result = _get_affected_months(df)
    
    assert len(result) == 3
    assert (2024, 1) in result
    assert (2024, 2) in result
    assert (2025, 1) in result


def test_get_affected_months_deduplicates(catalog):
    """Test that _get_affected_months deduplicates months."""
    df = pd.DataFrame({
        "year": [2024, 2024, 2024],
        "month": [1, 1, 1],
        "value": [1.0, 2.0, 3.0],
    })
    
    result = _get_affected_months(df)
    
    assert len(result) == 1
    assert (2024, 1) in result


def test_is_already_consolidated_true(catalog):
    """Test checking if month is already consolidated (returns True)."""
    # Mock manifest with completed status
    catalog.read_consolidation_manifest = Mock(return_value={"status": "completed"})
    
    result = _is_already_consolidated(catalog, "test_dataset", 2024, 1)
    
    assert result is True
    catalog.read_consolidation_manifest.assert_called_once_with("test_dataset", 2024, 1)


def test_is_already_consolidated_false_no_manifest(catalog):
    """Test checking if month is already consolidated (no manifest, returns False)."""
    # Mock manifest not found
    catalog.read_consolidation_manifest = Mock(return_value=None)
    
    result = _is_already_consolidated(catalog, "test_dataset", 2024, 1)
    
    assert result is False


def test_is_already_consolidated_false_in_progress(catalog):
    """Test checking if month is already consolidated (in_progress, returns False)."""
    # Mock manifest with in_progress status
    catalog.read_consolidation_manifest = Mock(return_value={"status": "in_progress"})
    
    result = _is_already_consolidated(catalog, "test_dataset", 2024, 1)
    
    assert result is False


def test_consolidate_month_idempotency(catalog, dataset_config, sample_data):
    """Test that _consolidate_month skips if already consolidated."""
    # Mock already consolidated
    catalog.read_consolidation_manifest = Mock(return_value={"status": "completed"})
    
    # Should not call consolidation methods
    catalog.cleanup_temp_projections = Mock()
    catalog.write_consolidation_manifest = Mock()
    catalog.list_events_for_month = Mock()
    
    _consolidate_month(catalog, dataset_config, 2024, 1, ["obs_time", "internal_series_code"])
    
    # Should skip consolidation
    catalog.cleanup_temp_projections.assert_not_called()
    catalog.write_consolidation_manifest.assert_not_called()
    catalog.list_events_for_month.assert_not_called()


def test_consolidate_month_cleanup_temp_on_start(catalog, dataset_config, sample_data):
    """Test that _consolidate_month cleans up temp files on start."""
    # Mock not consolidated
    catalog.read_consolidation_manifest = Mock(return_value=None)
    catalog.cleanup_temp_projections = Mock()
    catalog.write_consolidation_manifest = Mock()
    
    # Mock consolidation to return empty (early return)
    catalog.list_events_for_month = Mock(return_value=[])
    
    _consolidate_month(catalog, dataset_config, 2024, 1, ["obs_time", "internal_series_code"])
    
    # Should cleanup temp files
    catalog.cleanup_temp_projections.assert_called_once_with("test_dataset", 2024, 1)


def test_consolidate_month_cleanup_temp_on_error(catalog, dataset_config, sample_data):
    """Test that _consolidate_month cleans up temp files on error."""
    # Mock not consolidated
    catalog.read_consolidation_manifest = Mock(return_value=None)
    catalog.cleanup_temp_projections = Mock()
    catalog.write_consolidation_manifest = Mock()
    
    # Mock consolidation to raise error
    catalog.list_events_for_month = Mock(side_effect=Exception("Consolidation error"))
    
    with pytest.raises(Exception, match="Consolidation error"):
        _consolidate_month(catalog, dataset_config, 2024, 1, ["obs_time", "internal_series_code"])
    
    # Should cleanup temp files twice (on start and on error)
    assert catalog.cleanup_temp_projections.call_count == 2


def test_consolidate_month_manifest_lifecycle(catalog, dataset_config, sample_data):
    """Test manifest lifecycle (in_progress -> completed)."""
    # Mock not consolidated
    catalog.read_consolidation_manifest = Mock(return_value=None)
    catalog.cleanup_temp_projections = Mock()
    catalog.write_consolidation_manifest = Mock()
    
    # Mock successful consolidation
    catalog.list_events_for_month = Mock(return_value=["event1.parquet"])
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.parquet_io.read_from_bytes = Mock(return_value=sample_data)
    catalog.write_series_projection_temp = Mock()
    catalog.move_series_projection_from_temp = Mock()
    
    _consolidate_month(catalog, dataset_config, 2024, 1, ["obs_time", "internal_series_code"])
    
    # Should write manifest twice: in_progress and completed
    assert catalog.write_consolidation_manifest.call_count == 2
    catalog.write_consolidation_manifest.assert_any_call("test_dataset", 2024, 1, status="in_progress")
    catalog.write_consolidation_manifest.assert_any_call("test_dataset", 2024, 1, status="completed")


def test_consolidate_month_no_series_projections(catalog, dataset_config):
    """Test _consolidate_month when no series projections are returned."""
    # Mock not consolidated
    catalog.read_consolidation_manifest = Mock(return_value=None)
    catalog.cleanup_temp_projections = Mock()
    catalog.write_consolidation_manifest = Mock()
    
    # Mock consolidation to return empty dict
    catalog.list_events_for_month = Mock(return_value=[])
    
    _consolidate_month(catalog, dataset_config, 2024, 1, ["obs_time", "internal_series_code"])
    
    # Should mark as in_progress but not completed (early return)
    catalog.write_consolidation_manifest.assert_called_once_with("test_dataset", 2024, 1, status="in_progress")


def test_read_consolidation_manifest_success(catalog):
    """Test reading consolidation manifest successfully."""
    manifest_data = {
        "dataset_id": "test_dataset",
        "year": 2024,
        "month": 1,
        "status": "completed",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    
    catalog._read_json = Mock(return_value=manifest_data)
    
    result = catalog.read_consolidation_manifest("test_dataset", 2024, 1)
    
    assert result == manifest_data
    catalog._read_json.assert_called_once()


def test_read_consolidation_manifest_not_found(catalog):
    """Test reading consolidation manifest when not found."""
    catalog._read_json = Mock(return_value=None)
    
    result = catalog.read_consolidation_manifest("test_dataset", 2024, 1)
    
    assert result is None


def test_write_consolidation_manifest(catalog):
    """Test writing consolidation manifest."""
    catalog.s3.put_object = Mock()
    
    catalog.write_consolidation_manifest("test_dataset", 2024, 1, "completed")
    
    # Should write manifest
    assert catalog.s3.put_object.called
    call_args = catalog.s3.put_object.call_args
    assert call_args[0][0] == "datasets-test/test_dataset/projections/consolidation/2024/01/manifest.json"
    assert call_args[1]["content_type"] == "application/json"


def test_cleanup_temp_projections(catalog):
    """Test cleaning up temporary projections."""
    # Mock list_objects to return temp keys
    catalog.s3.list_objects = Mock(return_value=[
        "datasets-test/test_dataset/projections/windows/SERIES_1/year=2024/month=01/.tmp/data.parquet",
        "datasets-test/test_dataset/projections/windows/SERIES_1/year=2024/month=01/data.parquet",
        "datasets-test/test_dataset/projections/windows/SERIES_2/year=2024/month=01/.tmp/data.parquet",
    ])
    
    catalog.s3.s3_client.delete_object = Mock()
    
    catalog.cleanup_temp_projections("test_dataset", 2024, 1)
    
    # Should delete only temp files (2 temp files)
    assert catalog.s3.s3_client.delete_object.call_count == 2


def test_cleanup_temp_projections_no_temp_files(catalog):
    """Test cleaning up when no temp files exist."""
    # Mock list_objects to return no temp keys
    catalog.s3.list_objects = Mock(return_value=[
        "datasets-test/test_dataset/projections/windows/SERIES_1/year=2024/month=01/data.parquet",
    ])
    
    catalog.s3.s3_client.delete_object = Mock()
    
    catalog.cleanup_temp_projections("test_dataset", 2024, 1)
    
    # Should not delete anything
    catalog.s3.s3_client.delete_object.assert_not_called()


def test_move_series_projection_from_temp(catalog):
    """Test moving series projection from temp to final location."""
    # Mock get_object to return data
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.s3.put_object = Mock()
    catalog.s3.s3_client.delete_object = Mock()
    
    catalog.move_series_projection_from_temp("test_dataset", "SERIES_1", 2024, 1)
    
    # Should copy from temp to final
    assert catalog.s3.put_object.called
    # Should delete temp
    assert catalog.s3.s3_client.delete_object.called


def test_move_series_projection_from_temp_delete_ignores_error(catalog):
    """Test that move ignores error when deleting temp file."""
    # Mock get_object and put_object to succeed
    catalog.s3.get_object = Mock(return_value=b"parquet-data")
    catalog.s3.put_object = Mock()
    # Mock delete to raise error
    catalog.s3.s3_client.delete_object = Mock(side_effect=Exception("Delete error"))
    
    # Should not raise error
    catalog.move_series_projection_from_temp("test_dataset", "SERIES_1", 2024, 1)
    
    # Should still succeed
    assert catalog.s3.put_object.called

