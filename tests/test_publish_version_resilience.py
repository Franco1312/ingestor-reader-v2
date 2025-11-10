"""Tests for publish_version resilience and consistency verification."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3
import pandas as pd
from datetime import datetime, timezone

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.domain.entities.manifest import Manifest, OutputsInfo, IndexInfo, SourceFile
from ingestor_reader.use_cases.steps.publish_version import publish_version


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


def test_verify_pointer_index_consistency_detects_inconsistency(catalog):
    """Test that verify_pointer_index_consistency detects when pointer and index are out of sync."""
    dataset_id = "test_dataset"
    version_ts = "2024-01-01T00-00-00"
    
    # Write pointer pointing to version_ts
    pointer_body = {
        "dataset_id": dataset_id,
        "current_version": version_ts,
    }
    catalog.put_current_manifest_pointer(dataset_id, pointer_body, None)
    
    # Write index with OLD version (inconsistency)
    old_index_df = pd.DataFrame({
        "key_hash": ["hash1", "hash2"],
        "version": ["2023-12-01T00-00-00", "2023-12-01T00-00-00"]
    })
    catalog.write_index(dataset_id, old_index_df)
    
    # Verify consistency should detect inconsistency
    is_consistent = catalog.verify_pointer_index_consistency(dataset_id)
    assert is_consistent is False


def test_verify_pointer_index_consistency_detects_consistency(catalog):
    """Test that verify_pointer_index_consistency detects when pointer and index are in sync."""
    dataset_id = "test_dataset"
    version_ts = "2024-01-01T00-00-00"
    
    # Write pointer pointing to version_ts
    pointer_body = {
        "dataset_id": dataset_id,
        "current_version": version_ts,
    }
    catalog.put_current_manifest_pointer(dataset_id, pointer_body, None)
    
    # Write event manifest
    manifest = Manifest(
        dataset_id=dataset_id,
        version=version_ts,
        created_at=datetime.now(timezone.utc).isoformat(),
        source={"files": []},
        outputs=OutputsInfo(
            data_prefix=f"datasets/{dataset_id}/events/{version_ts}/data/",
            files=[],
            rows_total=2,
            rows_added_this_version=2,
        ),
        index=IndexInfo(
            path=f"datasets/{dataset_id}/index/keys.parquet",
            key_columns=["series_code"],
            hash_column="key_hash",
        ),
    )
    catalog.write_event_manifest(dataset_id, version_ts, manifest)
    
    # Write index with correct number of rows (consistent)
    index_df = pd.DataFrame({
        "key_hash": ["hash1", "hash2"]
    })
    catalog.write_index(dataset_id, index_df)
    
    # Verify consistency should detect consistency
    is_consistent = catalog.verify_pointer_index_consistency(dataset_id)
    assert is_consistent is True


def test_rebuild_index_from_pointer_reconstructs_index(catalog):
    """Test that rebuild_index_from_pointer reconstructs index from pointer."""
    dataset_id = "test_dataset"
    version_ts = "2024-01-01T00-00-00"
    
    # Write pointer pointing to version_ts
    pointer_body = {
        "dataset_id": dataset_id,
        "current_version": version_ts,
    }
    catalog.put_current_manifest_pointer(dataset_id, pointer_body, None)
    
    # Write event manifest
    manifest = Manifest(
        dataset_id=dataset_id,
        version=version_ts,
        created_at=datetime.now(timezone.utc).isoformat(),
        source={"files": []},
        outputs=OutputsInfo(
            data_prefix=f"datasets/{dataset_id}/events/{version_ts}/data/",
            files=[],
            rows_total=2,
            rows_added_this_version=2,
        ),
        index=IndexInfo(
            path=f"datasets/{dataset_id}/index/keys.parquet",
            key_columns=["series_code"],
            hash_column="key_hash",
        ),
    )
    catalog.write_event_manifest(dataset_id, version_ts, manifest)
    
    # Write events (without version column - it's added during enrichment)
    event_df = pd.DataFrame({
        "series_code": ["A", "B"],
        "obs_time": pd.to_datetime(["2024-01-15", "2024-01-16"]),
        "value": [1, 2]
    })
    catalog.write_events(dataset_id, version_ts, event_df)
    
    # Rebuild index
    catalog.rebuild_index_from_pointer(dataset_id)
    
    # Verify index was rebuilt
    index_df = catalog.read_index(dataset_id)
    assert index_df is not None
    assert len(index_df) == 2
    assert "key_hash" in index_df.columns
    # Index only contains key_hash, not version


def test_publish_version_handles_index_write_failure_gracefully(catalog):
    """Test that publish_version handles index write failure after CAS."""
    dataset_id = "test_dataset"
    version_ts = "2024-01-01T00-00-00"
    
    # Write initial pointer
    initial_pointer = {
        "dataset_id": dataset_id,
        "current_version": "2023-12-01T00-00-00",
    }
    etag = catalog.put_current_manifest_pointer(dataset_id, initial_pointer, None)
    
    # Mock write_index to fail
    original_write = catalog.write_index
    
    def failing_write(dataset_id, df):
        raise Exception("Index write failed")
    
    catalog.write_index = failing_write
    
    # Publish version should handle failure gracefully
    source_file = SourceFile(sha256="hash123", size=1000)
    output_keys = ["events/2024-01-01T00-00-00/data/year=2024/month=01/part-0.parquet"]
    
    # Should raise exception but pointer should be updated (CAS succeeded)
    with pytest.raises(Exception, match="Index write failed"):
        publish_version(
            catalog=catalog,
            dataset_id=dataset_id,
            version_ts=version_ts,
            source_file=source_file,
            output_keys=output_keys,
            rows_added=1,
            primary_keys=["series_code"],
            current_index_df=None,
            delta_df=pd.DataFrame({"series_code": ["A"], "key_hash": ["hash1"]}),
            current_manifest_etag=etag,
        )
    
    # Verify pointer was updated (CAS succeeded)
    current_pointer = catalog.read_current_manifest(dataset_id)
    assert current_pointer["current_version"] == version_ts
    
    # Verify index was NOT updated (write failed)
    index_df = catalog.read_index(dataset_id)
    assert index_df is None  # Index doesn't exist or wasn't updated

