"""Application configuration entity."""
from pydantic import BaseModel


class AppConfig(BaseModel):
    """Application configuration for runtime environment."""
    s3_bucket: str
    aws_region: str | None = None
    sns_topic_arn: str | None = None
    dynamodb_lock_table: str | None = None
    """DynamoDB table name for distributed locks."""
    verify_ssl: bool = True

