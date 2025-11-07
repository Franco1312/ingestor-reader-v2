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
    
    def publish(
        self,
        topic_arn: str,
        message: dict[str, Any],  # type: ignore
        subject: str,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
    ) -> str:
        """
        Publish message to SNS topic.
        
        Args:
            topic_arn: SNS topic ARN
            message: Message payload (dict)
            subject: Message subject
            message_group_id: Message group ID for FIFO topics (optional)
            message_deduplication_id: Message deduplication ID for FIFO topics (optional)
            
        Returns:
            Message ID
        """
        message_json = json.dumps(message)
        publish_params = {
            "TopicArn": topic_arn,
            "Message": message_json,
            "Subject": subject,
        }
        
        if message_group_id is not None:
            publish_params["MessageGroupId"] = message_group_id
        if message_deduplication_id is not None:
            publish_params["MessageDeduplicationId"] = message_deduplication_id
        
        response = self.sns_client.publish(**publish_params)
        return response["MessageId"]

