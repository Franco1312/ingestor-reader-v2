"""Tests for write_events resilience."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3
import pandas as pd
from botocore.exceptions import ClientError

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.s3_storage import S3Storage


@pytest.fixture
def aws_resources():
    """Create AWS resources (S3 bucket) for testing."""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        yield {"s3_client": s3_client}


@pytest.fixture
def catalog(aws_resources):
    """Create S3Catalog instance for testing."""
    s3_storage = S3Storage(bucket="test-bucket", region="us-east-1")
    return S3Catalog(s3_storage)


def test_write_events_success_all_events_written(catalog):
    """Test that write_events succeeds when all events are written."""
    # Create test data with multiple months
    df = pd.DataFrame({
        "series_code": ["A", "B", "C"],
        "obs_time": pd.to_datetime(["2024-01-15", "2024-02-15", "2024-03-15"]),
        "value": [1, 2, 3]
    })
    
    event_keys = catalog.write_events("test_dataset", "2024-01-01T00-00-00", df)
    
    # Should write 3 event files (one per month)
    assert len(event_keys) == 3
    
    # Verify all events exist in S3
    for key in event_keys:
        obj = catalog.s3.get_object(key)
        assert obj is not None


def test_write_events_partial_failure_rolls_back(catalog):
    """Test that write_events rolls back if some events fail to write."""
    # Create test data with multiple months
    df = pd.DataFrame({
        "series_code": ["A", "B", "C"],
        "obs_time": pd.to_datetime(["2024-01-15", "2024-02-15", "2024-03-15"]),
        "value": [1, 2, 3]
    })
    
    # Mock put_object to fail on second call
    original_put = catalog.s3.put_object
    call_count = [0]
    
    def failing_put(key, body, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:  # Fail on second event
            raise ClientError({"Error": {"Code": "500"}}, "PutObject")
        return original_put(key, body, **kwargs)
    
    catalog.s3.put_object = failing_put
    
    # Should raise exception
    with pytest.raises(ClientError):
        catalog.write_events("test_dataset", "2024-01-01T00-00-00", df)
    
    # Verify rollback: first event was written but then deleted
    # Note: When the second event fails, the first event is in event_keys
    # and should be deleted by rollback
    prefix = "datasets/test_dataset/events/2024-01-01T00-00-00/data/"
    all_keys = catalog.s3.list_objects(prefix)
    # Rollback should have deleted the first event
    # If rollback worked, there should be 0 keys
    assert len(all_keys) == 0, f"Expected 0 keys after rollback, but found {len(all_keys)}: {all_keys}"


def test_write_events_index_update_failure_rolls_back_events(catalog):
    """Test that write_events rolls back events if index update fails."""
    # Create test data
    df = pd.DataFrame({
        "series_code": ["A"],
        "obs_time": pd.to_datetime(["2024-01-15"]),
        "value": [1]
    })
    
    # Mock _update_event_index to fail
    original_update = catalog._event_store._update_event_index
    
    def failing_update(dataset_id, year, month, version_ts):
        raise Exception("Index update failed")
    
    catalog._event_store._update_event_index = failing_update
    
    # Should raise exception
    with pytest.raises(Exception, match="Index update failed"):
        catalog.write_events("test_dataset", "2024-01-01T00-00-00", df)
    
    # Verify events were rolled back
    prefix = "datasets/test_dataset/events/2024-01-01T00-00-00/data/"
    all_keys = catalog.s3.list_objects(prefix)
    assert len(all_keys) == 0


def test_write_events_verifies_all_events_before_index_update(catalog):
    """Test that write_events verifies all events before updating index."""
    # Create test data with multiple months
    df = pd.DataFrame({
        "series_code": ["A", "B", "C"],
        "obs_time": pd.to_datetime(["2024-01-15", "2024-02-15", "2024-03-15"]),
        "value": [1, 2, 3]
    })
    
    # Track when index is updated
    index_updated = [False]
    original_update = catalog._event_store._update_event_index
    
    def track_update(dataset_id, year, month, version_ts):
        # Verify all events exist before updating index
        prefix = f"datasets/{dataset_id}/events/{version_ts}/data/"
        all_keys = catalog.s3.list_objects(prefix)
        expected_keys = [
            f"datasets/{dataset_id}/events/{version_ts}/data/year=2024/month=01/part-0.parquet",
            f"datasets/{dataset_id}/events/{version_ts}/data/year=2024/month=02/part-0.parquet",
            f"datasets/{dataset_id}/events/{version_ts}/data/year=2024/month=03/part-0.parquet",
        ]
        assert len(all_keys) == 3
        for expected_key in expected_keys:
            assert expected_key in all_keys
        index_updated[0] = True
        return original_update(dataset_id, year, month, version_ts)
    
    catalog._event_store._update_event_index = track_update
    
    event_keys = catalog.write_events("test_dataset", "2024-01-01T00-00-00", df)
    
    # Should have updated index
    assert index_updated[0] is True
    assert len(event_keys) == 3

