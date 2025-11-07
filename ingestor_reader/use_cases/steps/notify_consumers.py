"""Notify consumers step."""
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

from ingestor_reader.infra.event_bus.sns_publisher import SNSPublisher

logger = logging.getLogger(__name__)


def _is_fifo_topic(topic_arn: str) -> bool:
    """Check if topic is FIFO."""
    return topic_arn.endswith(".fifo")


def _build_fifo_parameters(
    topic_arn: str,
    dataset_id: str,
    manifest_pointer: str,
) -> tuple[Optional[str], Optional[str]]:
    """
    Build FIFO parameters for SNS publish.
    
    Args:
        topic_arn: SNS topic ARN
        dataset_id: Dataset ID
        manifest_pointer: Path to the published manifest
        
    Returns:
        Tuple of (message_group_id, message_deduplication_id) or (None, None) if not FIFO
    """
    if not _is_fifo_topic(topic_arn):
        return None, None
    
    message_group_id = dataset_id
    message_deduplication_id = hashlib.sha256(manifest_pointer.encode()).hexdigest()
    return message_group_id, message_deduplication_id


def notify_consumers(
    publisher: SNSPublisher,
    topic_arn: Optional[str],
    dataset_id: str,
    manifest_pointer: str,
) -> None:
    """
    Notify consumers of new version.
    
    Args:
        publisher: SNS publisher instance
        topic_arn: SNS topic ARN (None to skip)
        dataset_id: Dataset ID
        manifest_pointer: Path to the published manifest
    """
    if not topic_arn:
        logger.info("Skipping notification: no topic ARN")
        return
    
    logger.info("Notifying consumers: %s manifest=%s", dataset_id, manifest_pointer)
    
    event = {
        "type": "DATASET_UPDATED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset_id": dataset_id,
        "manifest_pointer": manifest_pointer,
    }
    
    message_group_id, message_deduplication_id = _build_fifo_parameters(
        topic_arn, dataset_id, manifest_pointer
    )
    
    publisher.publish(
        topic_arn=topic_arn,
        message=event,
        subject="DATASET_UPDATED",
        message_group_id=message_group_id,
        message_deduplication_id=message_deduplication_id,
    )
    logger.info("Notification sent")

