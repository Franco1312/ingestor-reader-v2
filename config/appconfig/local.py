"""Local environment configuration."""
import os
from ingestor_reader.domain.entities.app_config import AppConfig

config = AppConfig(
    s3_bucket="ingestor-datasets",
    dynamodb_table=os.getenv("DYNAMODB_TABLE"),
    aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    sns_topic_arn="arn:aws:sns:us-east-1:706341500093:ingestor-reader-write-update.fifo",
    verify_ssl=False,
)

