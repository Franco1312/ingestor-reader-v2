"""Base S3 store with common operations."""
import json
from typing import Optional
import pandas as pd
from botocore.exceptions import ClientError

from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.parquet_io import ParquetIO
from ingestor_reader.infra.common.paths import S3PathBuilder
from ingestor_reader.infra.common.clock import Clock, get_clock


class S3BaseStore:
    """Base class for S3 stores with common operations."""
    
    def __init__(
        self,
        s3_storage: S3Storage,
        paths: S3PathBuilder | None = None,
        clock: Clock | None = None,
    ):
        """
        Initialize base store.
        
        Args:
            s3_storage: S3 storage instance
            paths: Path builder instance (defaults to S3PathBuilder)
            clock: Clock instance (defaults to system clock)
        """
        self.s3 = s3_storage
        self.parquet_io = ParquetIO()
        self.paths = paths or S3PathBuilder()
        self.clock = clock or get_clock()
    
    @staticmethod
    def _is_not_found_error(error: ClientError) -> bool:
        """Check if ClientError is a 404/NoSuchKey error."""
        error_code = error.response.get("Error", {}).get("Code", "")
        return error_code in ("404", "NoSuchKey")
    
    def _read_json(self, key: str) -> Optional[dict]:
        """Read JSON object from S3 with error handling."""
        try:
            body = self.s3.get_object(key)
            return json.loads(body.decode())
        except ClientError as e:
            if self._is_not_found_error(e):
                return None
            raise
        except json.JSONDecodeError:
            return None
    
    def _read_parquet(self, key: str) -> Optional[pd.DataFrame]:
        """Read Parquet object from S3 with error handling."""
        try:
            body = self.s3.get_object(key)
            return self.parquet_io.read_from_bytes(body)
        except ClientError as e:
            if self._is_not_found_error(e):
                return None
            raise
    
    def _write_json(self, key: str, data: dict) -> None:
        """Write JSON object to S3."""
        body = json.dumps(data, indent=2).encode()
        self.s3.put_object(key, body, content_type="application/json")
    
    def _write_parquet(self, key: str, df: pd.DataFrame) -> None:
        """Write Parquet object to S3."""
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")

