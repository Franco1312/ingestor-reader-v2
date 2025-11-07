"""DynamoDB lock implementation."""
import boto3
import logging
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from typing import Optional

logger = logging.getLogger(__name__)


class DynamoDBLock:
    """DynamoDB-based lock for concurrency control."""
    
    def __init__(self, table_name: str, region: Optional[str] = None):
        """
        Initialize DynamoDB lock.
        
        Args:
            table_name: DynamoDB table name
            region: AWS region
        """
        self.table_name = table_name
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.table = self.dynamodb.Table(table_name)
    
    def acquire(self, dataset_id: str, run_id: str, ttl_seconds: int = 3600) -> bool:
        """
        Acquire lock for dataset.
        
        Args:
            dataset_id: Dataset identifier
            run_id: Run identifier
            ttl_seconds: Lock TTL in seconds
            
        Returns:
            True if lock acquired, False otherwise
        """
        now = datetime.now(timezone.utc)
        expires_at = int((now + timedelta(seconds=ttl_seconds)).timestamp())
        
        try:
            self.table.put_item(
                Item={
                    "dataset_id": dataset_id,
                    "run_id": run_id,
                    "expires_at": expires_at,
                    "acquired_at": now.isoformat(),
                },
                ConditionExpression="attribute_not_exists(dataset_id) OR expires_at < :now",
                ExpressionAttributeValues={":now": int(now.timestamp())},
            )
            logger.info("Lock acquired for %s (run_id=%s, expires_at=%d)", dataset_id, run_id, expires_at)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                try:
                    response = self.table.get_item(Key={"dataset_id": dataset_id})
                    if "Item" in response:
                        existing_run_id = response["Item"].get("run_id", "unknown")
                        existing_expires = response["Item"].get("expires_at", 0)
                        logger.warning(
                            "Lock already exists for %s (run_id=%s, expires_at=%d, current_time=%d)",
                            dataset_id, existing_run_id, existing_expires, int(now.timestamp())
                        )
                except Exception:
                    pass
                return False
            raise
    
    def release(self, dataset_id: str, run_id: str) -> None:
        """
        Release lock for dataset.
        
        Args:
            dataset_id: Dataset identifier
            run_id: Run identifier
        """
        try:
            # Try to delete with run_id match first
            self.table.delete_item(
                Key={"dataset_id": dataset_id},
                ConditionExpression="run_id = :run_id",
                ExpressionAttributeValues={":run_id": run_id},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # If run_id doesn't match, try to delete if lock is expired
                now = datetime.now(timezone.utc)
                try:
                    self.table.delete_item(
                        Key={"dataset_id": dataset_id},
                        ConditionExpression="expires_at < :now",
                        ExpressionAttributeValues={":now": int(now.timestamp())},
                    )
                    logger.warning("Released expired lock for %s (run_id mismatch)", dataset_id)
                except ClientError as e2:
                    if e2.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise
            else:
                raise

