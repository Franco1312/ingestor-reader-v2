"""Production environment configuration."""
import os
from ingestor_reader.domain.entities.app_config import AppConfig

config = AppConfig(
    s3_bucket=os.getenv("S3_BUCKET", "ingestor-datasets"),
    aws_region=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    sns_topic_arn=os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:706341500093:ingestor-reader-write-update.fifo"),
    verify_ssl=os.getenv("VERIFY_SSL", "true").lower() == "true",
)

