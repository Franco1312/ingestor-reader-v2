"""Tests for DynamoDB lock implementation."""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError

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
def lock_manager(dynamodb_table):
    """Create a DynamoDBLock instance for testing."""
    return DynamoDBLock(table_name="test-locks", region="us-east-1", ttl_seconds=3600)


def test_acquire_lock_success(lock_manager):
    """Test successfully acquiring a lock."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    result = lock_manager.acquire(lock_key, owner_id)
    
    assert result is True
    
    # Verify lock exists in DynamoDB
    response = lock_manager.table.get_item(Key={"lock_key": lock_key})
    assert "Item" in response
    item = response["Item"]
    assert item["lock_key"] == lock_key
    assert item["owner_id"] == owner_id
    assert "expires_at" in item
    assert "acquired_at" in item


def test_acquire_lock_already_locked(lock_manager):
    """Test acquiring a lock that is already held."""
    lock_key = "pipeline:test_dataset"
    owner_id_1 = "run-123"
    owner_id_2 = "run-456"
    
    # First acquisition succeeds
    result1 = lock_manager.acquire(lock_key, owner_id_1)
    assert result1 is True
    
    # Second acquisition fails (lock already held)
    result2 = lock_manager.acquire(lock_key, owner_id_2)
    assert result2 is False
    
    # Verify first owner still holds the lock
    response = lock_manager.table.get_item(Key={"lock_key": lock_key})
    assert response["Item"]["owner_id"] == owner_id_1


def test_acquire_lock_expired(lock_manager):
    """Test acquiring a lock that has expired."""
    lock_key = "pipeline:test_dataset"
    owner_id_1 = "run-123"
    owner_id_2 = "run-456"
    
    # Acquire lock with short TTL
    lock_manager_short_ttl = DynamoDBLock(
        table_name="test-locks",
        region="us-east-1",
        ttl_seconds=1  # 1 second TTL
    )
    
    result1 = lock_manager_short_ttl.acquire(lock_key, owner_id_1)
    assert result1 is True
    
    # Wait for lock to expire
    import time
    time.sleep(2)
    
    # Now second owner can acquire (lock expired)
    result2 = lock_manager.acquire(lock_key, owner_id_2)
    assert result2 is True
    
    # Verify second owner now holds the lock
    response = lock_manager.table.get_item(Key={"lock_key": lock_key})
    assert response["Item"]["owner_id"] == owner_id_2


def test_release_lock_success(lock_manager):
    """Test successfully releasing a lock."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Acquire lock
    lock_manager.acquire(lock_key, owner_id)
    
    # Release lock
    result = lock_manager.release(lock_key, owner_id)
    assert result is True
    
    # Verify lock no longer exists
    response = lock_manager.table.get_item(Key={"lock_key": lock_key})
    assert "Item" not in response


def test_release_lock_wrong_owner(lock_manager):
    """Test releasing a lock with wrong owner ID."""
    lock_key = "pipeline:test_dataset"
    owner_id_1 = "run-123"
    owner_id_2 = "run-456"
    
    # Acquire lock with owner 1
    lock_manager.acquire(lock_key, owner_id_1)
    
    # Try to release with owner 2 (should fail)
    result = lock_manager.release(lock_key, owner_id_2)
    assert result is False
    
    # Verify lock still exists with original owner
    response = lock_manager.table.get_item(Key={"lock_key": lock_key})
    assert response["Item"]["owner_id"] == owner_id_1


def test_release_lock_not_exists(lock_manager):
    """Test releasing a lock that doesn't exist."""
    lock_key = "pipeline:nonexistent"
    owner_id = "run-123"
    
    # Try to release non-existent lock
    result = lock_manager.release(lock_key, owner_id)
    assert result is False


def test_is_locked_active(lock_manager):
    """Test checking if an active lock exists."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Lock doesn't exist yet
    assert lock_manager.is_locked(lock_key) is False
    
    # Acquire lock
    lock_manager.acquire(lock_key, owner_id)
    
    # Lock should be active
    assert lock_manager.is_locked(lock_key) is True


def test_is_locked_expired(lock_manager):
    """Test checking if an expired lock exists."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Acquire lock with short TTL
    lock_manager_short_ttl = DynamoDBLock(
        table_name="test-locks",
        region="us-east-1",
        ttl_seconds=1
    )
    lock_manager_short_ttl.acquire(lock_key, owner_id)
    
    # Lock should be active initially
    assert lock_manager.is_locked(lock_key) is True
    
    # Wait for lock to expire
    import time
    time.sleep(2)
    
    # Lock should be expired
    assert lock_manager.is_locked(lock_key) is False


def test_is_locked_not_exists(lock_manager):
    """Test checking if a non-existent lock exists."""
    lock_key = "pipeline:nonexistent"
    
    assert lock_manager.is_locked(lock_key) is False


def test_acquire_release_cycle(lock_manager):
    """Test complete acquire-release cycle."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Acquire
    assert lock_manager.acquire(lock_key, owner_id) is True
    assert lock_manager.is_locked(lock_key) is True
    
    # Release
    assert lock_manager.release(lock_key, owner_id) is True
    assert lock_manager.is_locked(lock_key) is False
    
    # Can acquire again
    assert lock_manager.acquire(lock_key, owner_id) is True
    assert lock_manager.is_locked(lock_key) is True


def test_multiple_locks_different_keys(lock_manager):
    """Test multiple locks with different keys."""
    lock_key_1 = "pipeline:dataset1"
    lock_key_2 = "pipeline:dataset2"
    owner_id = "run-123"
    
    # Both can be acquired simultaneously
    assert lock_manager.acquire(lock_key_1, owner_id) is True
    assert lock_manager.acquire(lock_key_2, owner_id) is True
    
    # Both should be locked
    assert lock_manager.is_locked(lock_key_1) is True
    assert lock_manager.is_locked(lock_key_2) is True
    
    # Release both
    assert lock_manager.release(lock_key_1, owner_id) is True
    assert lock_manager.release(lock_key_2, owner_id) is True
    
    # Both should be unlocked
    assert lock_manager.is_locked(lock_key_1) is False
    assert lock_manager.is_locked(lock_key_2) is False


def test_acquire_with_dynamodb_error(lock_manager):
    """Test acquire when DynamoDB returns an unexpected error."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Mock DynamoDB to raise an unexpected error
    with patch.object(lock_manager.table, "put_item") as mock_put:
        mock_put.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}},
            "PutItem"
        )
        
        with pytest.raises(ClientError):
            lock_manager.acquire(lock_key, owner_id)


def test_release_with_dynamodb_error(lock_manager):
    """Test release when DynamoDB returns an unexpected error."""
    lock_key = "pipeline:test_dataset"
    owner_id = "run-123"
    
    # Acquire lock first
    lock_manager.acquire(lock_key, owner_id)
    
    # Mock DynamoDB to raise an unexpected error
    with patch.object(lock_manager.table, "delete_item") as mock_delete:
        mock_delete.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}},
            "DeleteItem"
        )
        
        with pytest.raises(ClientError):
            lock_manager.release(lock_key, owner_id)

