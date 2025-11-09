"""Tests for pipeline locks."""
import pytest
from unittest.mock import Mock, MagicMock, patch
from moto import mock_aws
import boto3
import pandas as pd

from ingestor_reader.domain.entities.app_config import AppConfig
from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.use_cases.run_pipeline import run_pipeline
from ingestor_reader.infra.locks.dynamodb_lock import DynamoDBLock


@pytest.fixture
def dynamodb_table():
    """Create a DynamoDB table for testing."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-locks",
            KeySchema=[{"AttributeName": "lock_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "lock_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


@pytest.fixture
def app_config_with_lock(dynamodb_table):
    """Create AppConfig with lock table configured."""
    return AppConfig(
        s3_bucket="test-bucket",
        aws_region="us-east-1",
        dynamodb_lock_table="test-locks",
    )


@pytest.fixture
def app_config_without_lock():
    """Create AppConfig without lock table configured."""
    return AppConfig(
        s3_bucket="test-bucket",
        aws_region="us-east-1",
        dynamodb_lock_table=None,
    )


@pytest.fixture
def dataset_config():
    """Create a minimal DatasetConfig for testing."""
    from ingestor_reader.domain.entities.dataset_config import (
        SourceConfig,
        ParseConfig,
        NormalizeConfig,
        OutputConfig,
    )
    return DatasetConfig(
        dataset_id="test_dataset",
        frequency="daily",
        lag_days=0,
        source=SourceConfig(kind="http", url="http://example.com/data.xlsx", format="xlsx"),
        parse=ParseConfig(plugin="test_parser"),
        normalize=NormalizeConfig(plugin="test_normalizer", primary_keys=["key"]),
        output=OutputConfig(),
    )


@patch("ingestor_reader.use_cases.run_pipeline.step_fetch_resource")
@patch("ingestor_reader.use_cases.run_pipeline.step_check_source_changed")
@patch("ingestor_reader.use_cases.run_pipeline.step_parse_file")
@patch("ingestor_reader.use_cases.run_pipeline.step_filter_new_data")
@patch("ingestor_reader.use_cases.run_pipeline.step_normalize_rows")
@patch("ingestor_reader.use_cases.run_pipeline.step_compute_delta")
@patch("ingestor_reader.use_cases.run_pipeline.step_enrich_metadata")
@patch("ingestor_reader.use_cases.run_pipeline.step_write_events")
@patch("ingestor_reader.use_cases.run_pipeline.step_publish_version")
@patch("ingestor_reader.use_cases.run_pipeline.step_consolidate_projection")
@patch("ingestor_reader.use_cases.run_pipeline.step_notify_consumers")
def test_pipeline_with_lock_acquires_and_releases(
    mock_notify,
    mock_consolidate,
    mock_publish,
    mock_write_events,
    mock_enrich,
    mock_compute_delta,
    mock_normalize,
    mock_filter,
    mock_parse,
    mock_check_source,
    mock_fetch,
    app_config_with_lock,
    dataset_config,
    dynamodb_table,
):
    """Test that pipeline acquires and releases lock correctly."""
    # Setup mocks with proper DataFrame returns
    mock_fetch.return_value = (b"content", "hash123", 100)
    mock_check_source.return_value = True
    mock_parse.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_filter.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_normalize.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_compute_delta.return_value = (pd.DataFrame({"col1": [1, 2, 3]}), pd.DataFrame({"key_hash": ["h1", "h2"]}))
    mock_enrich.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_write_events.return_value = (["key1"], 10)
    mock_publish.return_value = True
    
    # Run pipeline
    run = run_pipeline(dataset_config, app_config_with_lock, run_id="run-123")
    
    # Verify lock was acquired
    lock_manager = DynamoDBLock(table_name="test-locks", region="us-east-1")
    lock_key = "pipeline:test_dataset"
    assert lock_manager.is_locked(lock_key) is False  # Lock should be released
    
    # Verify pipeline executed
    mock_fetch.assert_called_once()
    mock_publish.assert_called_once()


@patch("ingestor_reader.use_cases.run_pipeline.step_fetch_resource")
def test_pipeline_with_lock_already_acquired_skips(
    mock_fetch,
    app_config_with_lock,
    dataset_config,
    dynamodb_table,
):
    """Test that pipeline skips execution if lock is already acquired."""
    # Acquire lock manually
    lock_manager = DynamoDBLock(table_name="test-locks", region="us-east-1")
    lock_key = "pipeline:test_dataset"
    lock_manager.acquire(lock_key, "other-run-456")
    
    # Run pipeline (should skip)
    run = run_pipeline(dataset_config, app_config_with_lock, run_id="run-123")
    
    # Verify pipeline did not execute
    mock_fetch.assert_not_called()
    
    # Verify lock still held by other run
    assert lock_manager.is_locked(lock_key) is True
    
    # Cleanup
    lock_manager.release(lock_key, "other-run-456")


@patch("ingestor_reader.use_cases.run_pipeline.step_fetch_resource")
@patch("ingestor_reader.use_cases.run_pipeline.step_check_source_changed")
def test_pipeline_without_lock_executes(
    mock_check_source,
    mock_fetch,
    app_config_without_lock,
    dataset_config,
):
    """Test that pipeline executes without lock if not configured."""
    # Setup mocks
    mock_fetch.return_value = (b"content", "hash123", 100)
    mock_check_source.return_value = False  # No changes, early return
    
    # Run pipeline
    run = run_pipeline(dataset_config, app_config_without_lock, run_id="run-123")
    
    # Verify pipeline executed (even without lock)
    mock_fetch.assert_called_once()
    mock_check_source.assert_called_once()


@patch("ingestor_reader.use_cases.run_pipeline.step_fetch_resource")
@patch("ingestor_reader.use_cases.run_pipeline.step_check_source_changed")
def test_pipeline_releases_lock_on_error(
    mock_check_source,
    mock_fetch,
    app_config_with_lock,
    dataset_config,
    dynamodb_table,
):
    """Test that pipeline releases lock even if an error occurs."""
    # Setup mocks to raise an error
    mock_fetch.return_value = (b"content", "hash123", 100)
    mock_check_source.side_effect = Exception("Test error")
    
    # Run pipeline (should raise error)
    with pytest.raises(Exception, match="Test error"):
        run_pipeline(dataset_config, app_config_with_lock, run_id="run-123")
    
    # Verify lock was released despite error
    lock_manager = DynamoDBLock(table_name="test-locks", region="us-east-1")
    lock_key = "pipeline:test_dataset"
    assert lock_manager.is_locked(lock_key) is False


@patch("ingestor_reader.use_cases.run_pipeline.step_fetch_resource")
@patch("ingestor_reader.use_cases.run_pipeline.step_check_source_changed")
@patch("ingestor_reader.use_cases.run_pipeline.step_parse_file")
@patch("ingestor_reader.use_cases.run_pipeline.step_filter_new_data")
@patch("ingestor_reader.use_cases.run_pipeline.step_normalize_rows")
@patch("ingestor_reader.use_cases.run_pipeline.step_compute_delta")
@patch("ingestor_reader.use_cases.run_pipeline.step_enrich_metadata")
@patch("ingestor_reader.use_cases.run_pipeline.step_write_events")
@patch("ingestor_reader.use_cases.run_pipeline.step_publish_version")
def test_pipeline_concurrent_execution_second_skips(
    mock_publish,
    mock_write_events,
    mock_enrich,
    mock_compute_delta,
    mock_normalize,
    mock_filter,
    mock_parse,
    mock_check_source,
    mock_fetch,
    app_config_with_lock,
    dataset_config,
    dynamodb_table,
):
    """Test that concurrent pipeline executions are prevented."""
    # Acquire lock manually before running pipeline (simulating concurrent execution)
    lock_manager = DynamoDBLock(table_name="test-locks", region="us-east-1")
    lock_key = "pipeline:test_dataset"
    lock_manager.acquire(lock_key, "other-run-999")
    
    # Setup mocks with proper DataFrame returns
    mock_fetch.return_value = (b"content", "hash123", 100)
    mock_check_source.return_value = True
    mock_parse.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_filter.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_normalize.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_compute_delta.return_value = (pd.DataFrame({"col1": [1, 2, 3]}), pd.DataFrame({"key_hash": ["h1", "h2"]}))
    mock_enrich.return_value = pd.DataFrame({"col1": [1, 2, 3]})
    mock_write_events.return_value = (["key1"], 10)
    mock_publish.return_value = True
    
    # Try to run pipeline while lock is held (should skip)
    run = run_pipeline(dataset_config, app_config_with_lock, run_id="run-123")
    
    # Verify pipeline did not execute (lock was already held)
    mock_fetch.assert_not_called()
    
    # Verify run was created but pipeline skipped
    assert run.run_id == "run-123"
    
    # Cleanup: release lock
    lock_manager.release(lock_key, "other-run-999")

