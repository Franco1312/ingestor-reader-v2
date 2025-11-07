"""Tests for delta service."""
import pandas as pd
import pytest

from ingestor_reader.domain.services.delta_service import (
    compute_delta,
    compute_key_hash,
    update_index,
)


def test_compute_key_hash():
    """Test key hash computation."""
    row = pd.Series({"obs_time": "2024-01-01", "code": "ABC"})
    hash_val = compute_key_hash(row, ["obs_time", "code"])
    assert isinstance(hash_val, str)
    assert len(hash_val) == 40  # SHA1 hex length


def test_compute_delta_first_run():
    """Test delta computation on first run (no index)."""
    df = pd.DataFrame({
        "obs_time": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
        "code": ["A", "B"],
    })
    
    delta = compute_delta(df, None, ["obs_time", "code"])
    
    assert len(delta) == 2
    assert "key_hash" in delta.columns
    assert set(delta["obs_time"]) == {"2024-01-01", "2024-01-02"}


def test_compute_delta_incremental():
    """Test delta computation with existing index."""
    normalized_df = pd.DataFrame({
        "obs_time": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "value": [1.0, 2.0, 3.0],
        "code": ["A", "B", "C"],
    })
    
    # First run
    delta1 = compute_delta(normalized_df, None, ["obs_time", "code"])
    assert len(delta1) == 3
    
    # Build index from first run
    index_df = delta1[["key_hash"]].copy()
    
    # Second run with same data
    delta2 = compute_delta(normalized_df, index_df, ["obs_time", "code"])
    assert len(delta2) == 0  # No new rows
    
    # Third run with new row
    new_df = pd.DataFrame({
        "obs_time": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "value": [1.0, 2.0, 3.0, 4.0],
        "code": ["A", "B", "C", "D"],
    })
    delta3 = compute_delta(new_df, index_df, ["obs_time", "code"])
    assert len(delta3) == 1  # Only new row
    assert delta3.iloc[0]["obs_time"] == "2024-01-04"


def test_compute_delta_idempotent():
    """Test that delta computation is idempotent."""
    df = pd.DataFrame({
        "obs_time": ["2024-01-01", "2024-01-02"],
        "value": [1.0, 2.0],
        "code": ["A", "B"],
    })
    
    # First run
    delta1 = compute_delta(df, None, ["obs_time", "code"])
    index1 = delta1[["key_hash"]].copy()
    
    # Second run with same data
    delta2 = compute_delta(df, index1, ["obs_time", "code"])
    assert len(delta2) == 0
    
    # Third run with same data again
    delta3 = compute_delta(df, index1, ["obs_time", "code"])
    assert len(delta3) == 0


def test_update_index():
    """Test index update."""
    # First run
    added1 = pd.DataFrame({
        "key_hash": ["hash1", "hash2"],
    })
    index1 = update_index(None, added1)
    assert len(index1) == 2
    
    # Second run with new rows
    added2 = pd.DataFrame({
        "key_hash": ["hash3", "hash4"],
    })
    index2 = update_index(index1, added2)
    assert len(index2) == 4
    assert set(index2["key_hash"]) == {"hash1", "hash2", "hash3", "hash4"}
    
    # Third run with duplicate
    added3 = pd.DataFrame({
        "key_hash": ["hash4", "hash5"],  # hash4 already exists
    })
    index3 = update_index(index2, added3)
    assert len(index3) == 5  # hash4 deduplicated
    assert set(index3["key_hash"]) == {"hash1", "hash2", "hash3", "hash4", "hash5"}

