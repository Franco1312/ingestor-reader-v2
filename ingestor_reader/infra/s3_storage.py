"""S3 storage operations."""
import boto3
from botocore.exceptions import ClientError
from typing import Optional


class S3Storage:
    """S3 storage adapter."""
    
    def __init__(self, bucket: str, region: Optional[str] = None):
        """
        Initialize S3 storage.
        
        Args:
            bucket: S3 bucket name
            region: AWS region (defaults to boto3 default)
        """
        self.bucket = bucket
        self.s3_client = boto3.client("s3", region_name=region)
    
    def get_object(self, key: str) -> bytes:
        """Get object from S3."""
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()
    
    def put_object(
        self,
        key: str,
        body: bytes,
        content_type: Optional[str] = None,
        if_match: Optional[str] = None,
    ) -> str:
        """
        Put object to S3 with optional conditional header.
        
        Args:
            key: S3 key
            body: Object body
            content_type: Content type
            if_match: ETag for conditional PUT (If-Match)
            
        Returns:
            ETag of uploaded object
            
        Raises:
            ClientError: If conditional check fails (412)
        """

        if if_match:
            current_meta = self.head_object(key)
            if current_meta is None:

                pass
            elif current_meta["ETag"] != if_match:
                raise ValueError("Conditional PUT failed: ETag mismatch")
        
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        
        try:
            response = self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                **extra_args
            )
            return response["ETag"].strip('"')
        except ClientError as e:
            raise
    
    def head_object(self, key: str) -> Optional[dict]:
        """
        Head object to get metadata.
        
        Returns:
            Metadata dict with ETag, or None if not found
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return {
                "ETag": response["ETag"].strip('"'),
                "ContentLength": response["ContentLength"],
            }
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return None
            raise
    
    def list_objects(self, prefix: str) -> list[str]:
        """List objects with prefix."""
        keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" in page:
                keys.extend([obj["Key"] for obj in page["Contents"]])
        return keys

