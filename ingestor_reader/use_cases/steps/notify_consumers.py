"""Notify consumers step."""
import logging
from typing import Optional

from ingestor_reader.infra.event_bus.sns_publisher import SNSPublisher

logger = logging.getLogger(__name__)


def notify_consumers(
    publisher: SNSPublisher,
    topic_arn: Optional[str],
    dataset_id: str,
    version_ts: str,
    rows_added: int,
) -> None:
    """
    Notify consumers of new version.
    
    Args:
        publisher: SNS publisher instance
        topic_arn: SNS topic ARN (None to skip)
        dataset_id: Dataset ID
        version_ts: Version timestamp
        rows_added: Number of rows added
    """
    if not topic_arn or rows_added == 0:
        logger.info("Skipping notification")
        return
    
    logger.info(f"Notifying consumers: {dataset_id} v{version_ts}")
    
    event = {
        "event_type": "DatasetVersionPublished",
        "dataset_id": dataset_id,
        "version": version_ts,
        "rows_added": rows_added,
    }
    
    publisher.publish_dataset_version_published(topic_arn, event)
    logger.info("Notification sent")

