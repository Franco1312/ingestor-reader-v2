"""DynamoDB lock implementation."""
import boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from typing import Optional


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
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
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
            self.table.delete_item(
                Key={"dataset_id": dataset_id},
                ConditionExpression="run_id = :run_id",
                ExpressionAttributeValues={":run_id": run_id},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

