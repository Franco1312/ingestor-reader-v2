"""DynamoDB lock implementation."""
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from ingestor_reader.infra.common import get_logger, get_clock

logger = get_logger(__name__)


class DynamoDBLock:
    """
    Distributed lock using DynamoDB.
    
    Uses conditional writes to ensure only one process can acquire a lock.
    Lock expires automatically after TTL to prevent deadlocks.
    """
    
    def __init__(self, table_name: str, region: Optional[str] = None, ttl_seconds: int = 3600):
        """
        Initialize DynamoDB lock.
        
        Args:
            table_name: DynamoDB table name
            region: AWS region (defaults to boto3 default)
            ttl_seconds: Lock TTL in seconds (default: 1 hour)
        """
        self.table_name = table_name
        self.ttl_seconds = ttl_seconds
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)
    
    def acquire(self, lock_key: str, owner_id: str) -> bool:
        """
        Acquire a lock.
        
        Args:
            lock_key: Lock key (e.g., dataset_id)
            owner_id: Unique identifier for this lock owner (e.g., run_id)
            
        Returns:
            True if lock acquired, False if already locked
        """
        try:
            clock = get_clock()
            now = int(clock.now().timestamp())
            expires_at = now + self.ttl_seconds
            

            self.table.put_item(
                Item={
                    "lock_key": lock_key,
                    "owner_id": owner_id,
                    "expires_at": expires_at,
                    "acquired_at": now,
                },
                ConditionExpression="attribute_not_exists(lock_key) OR expires_at < :now",
                ExpressionAttributeValues={":now": now},
            )
            
            logger.info("Acquired lock for %s (owner: %s)", lock_key, owner_id)
            return True
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConditionalCheckFailedException":
                logger.warning("Lock already acquired for %s", lock_key)
                return False
            raise
    
    def release(self, lock_key: str, owner_id: str) -> bool:
        """
        Release a lock.
        
        Args:
            lock_key: Lock key
            owner_id: Owner ID (must match to release)
            
        Returns:
            True if released, False if lock doesn't exist or owner doesn't match
        """
        try:

            self.table.delete_item(
                Key={"lock_key": lock_key},
                ConditionExpression="owner_id = :owner",
                ExpressionAttributeValues={":owner": owner_id},
            )
            
            logger.info("Released lock for %s (owner: %s)", lock_key, owner_id)
            return True
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ConditionalCheckFailedException":
                logger.warning("Lock not found or owner mismatch for %s", lock_key)
                return False
            raise
    
    def is_locked(self, lock_key: str) -> bool:
        """
        Check if a lock is currently held.
        
        Args:
            lock_key: Lock key
            
        Returns:
            True if locked, False otherwise
        """
        try:
            response = self.table.get_item(Key={"lock_key": lock_key})
            
            if "Item" not in response:
                return False
            
            item = response["Item"]
            expires_at = item.get("expires_at", 0)
            clock = get_clock()
            now = int(clock.now().timestamp())
            

            if expires_at < now:
                return False
            
            return True
            
        except ClientError:
            return False
    

