"""SNS event publisher."""
import json
import boto3
from typing import Optional, Any


class SNSPublisher:
    """SNS event publisher."""
    
    def __init__(self, region: Optional[str] = None):
        """
        Initialize SNS publisher.
        
        Args:
            region: AWS region
        """
        self.sns_client = boto3.client("sns", region_name=region)
    
    def publish_dataset_version_published(
        self, topic_arn: str, event_dict: dict[str, Any]  # type: ignore
    ) -> str:
        """
        Publish dataset version published event.
        
        Args:
            topic_arn: SNS topic ARN
            event_dict: Event payload
            
        Returns:
            Message ID
        """
        message = json.dumps(event_dict)
        response = self.sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject="DatasetVersionPublished",
        )
        return response["MessageId"]

