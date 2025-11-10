"""S3 manifest store operations."""
import json
from typing import Optional
from botocore.exceptions import ClientError

from ingestor_reader.domain.entities.manifest import Manifest
from ingestor_reader.infra.s3_stores.base import S3BaseStore


class S3ManifestStore(S3BaseStore):
    """S3 store for manifest operations."""
    
    def get_current_manifest_etag(self, dataset_id: str) -> Optional[str]:
        """Get ETag of current manifest."""
        key = self.paths.current_manifest_key(dataset_id)
        metadata = self.s3.head_object(key)
        return metadata["ETag"] if metadata else None
    
    def read_current_manifest(self, dataset_id: str) -> Optional[dict]:
        """Read current manifest pointer."""
        key = self.paths.current_manifest_key(dataset_id)
        return self._read_json(key)
    
    def put_current_manifest_pointer(
        self, dataset_id: str, body: dict, if_match_etag: Optional[str]
    ) -> str:
        """Update current manifest pointer with CAS."""
        key = self.paths.current_manifest_key(dataset_id)
        body_bytes = json.dumps(body, indent=2).encode()
        try:
            return self.s3.put_object(
                key, body_bytes, content_type="application/json", if_match=if_match_etag
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "412":
                raise ValueError("Conditional PUT failed: ETag mismatch") from e
            raise
    
    def write_event_manifest(self, dataset_id: str, version_ts: str, manifest: Manifest) -> None:
        """Write event manifest."""
        key = self.paths.event_manifest_key(dataset_id, version_ts)
        body = manifest.model_dump_json(indent=2)
        self.s3.put_object(key, body.encode(), content_type="application/json")
    
    def read_event_manifest(self, dataset_id: str, version_ts: str) -> Optional[dict]:
        """Read event manifest."""
        key = self.paths.event_manifest_key(dataset_id, version_ts)
        return self._read_json(key)
    
    def get_event_manifest_pointer(self, dataset_id: str, version_ts: str) -> str:
        """Get manifest pointer path for an event (for SNS notifications)."""
        return self.paths.event_manifest_pointer(dataset_id, version_ts)

