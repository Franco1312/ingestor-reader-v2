"""Tests for S3 catalog paths."""
import json
import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd

from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.manifest import Manifest, OutputsInfo, IndexInfo


def test_s3_catalog_paths():
    """Test S3 catalog path construction."""
    s3_storage = Mock(spec=S3Storage)
    s3_storage.bucket = "test-bucket"
    catalog = S3Catalog(s3_storage)
    
    dataset_id = "TEST_DATASET"
    version_ts = "2024-01-01T00-00-00"
    
    # Test path methods using public methods
    config_key = catalog.paths.config_key(dataset_id)
    assert "datasets" in config_key
    assert dataset_id in config_key
    
    index_key = catalog.paths.index_key(dataset_id)
    assert "index" in index_key
    
    event_manifest_key = catalog.paths.event_manifest_key(dataset_id, version_ts)
    assert "events" in event_manifest_key
    
    current_manifest_key = catalog.paths.current_manifest_key(dataset_id)
    assert "current" in current_manifest_key
    
    events_prefix = catalog.paths.events_prefix(dataset_id, version_ts)
    assert "events" in events_prefix


def test_get_current_manifest_etag():
    """Test getting current manifest ETag."""
    s3_storage = Mock(spec=S3Storage)
    s3_storage.head_object = Mock(return_value={"ETag": "etag123"})
    catalog = S3Catalog(s3_storage)
    
    etag = catalog.get_current_manifest_etag("TEST")
    assert etag == "etag123"
    
    # Test None case
    s3_storage.head_object = Mock(return_value=None)
    etag = catalog.get_current_manifest_etag("TEST")
    assert etag is None


def test_read_current_manifest():
    """Test reading current manifest."""
    s3_storage = Mock(spec=S3Storage)
    manifest_data = {"dataset_id": "TEST", "current_version": "v1"}
    s3_storage.get_object = Mock(return_value=json.dumps(manifest_data).encode())
    catalog = S3Catalog(s3_storage)
    
    manifest = catalog.read_current_manifest("TEST")
    assert manifest == manifest_data
    
    # Test None case
    from botocore.exceptions import ClientError
    error = ClientError({"Error": {"Code": "404"}}, "GetObject")
    s3_storage.get_object = Mock(side_effect=error)
    manifest = catalog.read_current_manifest("TEST")
    assert manifest is None


def test_put_current_manifest_pointer_cas():
    """Test CAS pointer update."""
    s3_storage = Mock(spec=S3Storage)
    s3_storage.put_object = Mock(return_value="new-etag")
    catalog = S3Catalog(s3_storage)
    
    body = {"dataset_id": "TEST", "current_version": "v1"}
    etag = catalog.put_current_manifest_pointer("TEST", body, "old-etag")
    
    assert etag == "new-etag"
    s3_storage.put_object.assert_called_once()
    call_args = s3_storage.put_object.call_args
    assert call_args[1]["if_match"] == "old-etag"
    
    # Test CAS failure
    from botocore.exceptions import ClientError
    error = ClientError({"Error": {"Code": "412"}}, "PutObject")
    s3_storage.put_object = Mock(side_effect=error)
    
    with pytest.raises(ValueError, match="Conditional PUT failed"):
        catalog.put_current_manifest_pointer("TEST", body, "old-etag")


def test_write_event_manifest():
    """Test writing event manifest."""
    s3_storage = Mock(spec=S3Storage)
    s3_storage.put_object = Mock(return_value="etag")
    catalog = S3Catalog(s3_storage)
    
    manifest = Manifest(
        dataset_id="TEST",
        version="v1",
        created_at="2024-01-01T00:00:00Z",
        source={"files": []},
        outputs=OutputsInfo(
            data_prefix="prefix/",
            files=["file1.parquet"],
            rows_total=10,
            rows_added_this_version=10,
        ),
        index=IndexInfo(
            path="index/keys.parquet",
            key_columns=["key"],
            hash_column="key_hash",
        ),
    )
    
    catalog.write_event_manifest("TEST", "v1", manifest)
    s3_storage.put_object.assert_called_once()
    call_args = s3_storage.put_object.call_args
    assert "events" in call_args[0][0]  # key contains events


def test_read_write_index():
    """Test reading and writing index."""
    s3_storage = Mock(spec=S3Storage)
    catalog = S3Catalog(s3_storage)
    
    # Mock parquet IO on the internal store
    df = pd.DataFrame({"key_hash": ["hash1", "hash2"]})
    catalog._index_store.parquet_io = Mock()
    catalog._index_store.parquet_io.write_to_bytes = Mock(return_value=b"parquet-data")
    catalog._index_store.parquet_io.read_from_bytes = Mock(return_value=df)
    
    # Test write
    catalog.write_index("TEST", df)
    s3_storage.put_object.assert_called_once()
    
    # Test read
    s3_storage.get_object = Mock(return_value=b"parquet-data")
    result = catalog.read_index("TEST")
    assert len(result) == 2
    assert "key_hash" in result.columns
    
    # Test read None
    from botocore.exceptions import ClientError
    error = ClientError({"Error": {"Code": "404"}}, "GetObject")
    s3_storage.get_object = Mock(side_effect=error)
    result = catalog.read_index("TEST")
    assert result is None


def test_write_events():
    """Test writing events."""
    s3_storage = Mock(spec=S3Storage)
    catalog = S3Catalog(s3_storage)
    
    df = pd.DataFrame({"col1": [1, 2, 3]})
    catalog._event_store.parquet_io = Mock()
    catalog._event_store.parquet_io.write_to_bytes = Mock(return_value=b"parquet-data")
    
    keys = catalog.write_events("TEST", "v1", df)
    
    assert len(keys) == 1
    assert "events" in keys[0]
    assert "v1" in keys[0]
    s3_storage.put_object.assert_called_once()

