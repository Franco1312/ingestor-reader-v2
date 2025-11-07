"""Staging environment configuration."""
import os
from ingestor_reader.domain.entities.app_config import AppConfig

config = AppConfig(
    s3_bucket="ingestor-datasets",
    dynamodb_table=os.getenv("DYNAMODB_TABLE", "etl-locks-staging"),
    aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    sns_topic_arn=os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:datasets-staging"),
    verify_ssl=False,
)

