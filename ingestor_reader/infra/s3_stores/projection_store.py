"""S3 projection store operations."""
from typing import Optional
import pandas as pd
from botocore.exceptions import ClientError

from ingestor_reader.infra.s3_stores.base import S3BaseStore


class S3ProjectionStore(S3BaseStore):
    """S3 store for projection operations."""
    
    def read_series_projection(
        self, dataset_id: str, series_code: str, year: int, month: int
    ) -> Optional[pd.DataFrame]:
        """Read series projection."""
        key = self.paths.projection_series_key(dataset_id, series_code, year, month)
        return self._read_parquet(key)
    
    def write_series_projection(
        self, dataset_id: str, series_code: str, year: int, month: int, df: pd.DataFrame
    ) -> None:
        """Write series projection."""
        key = self.paths.projection_series_key(dataset_id, series_code, year, month)
        self._write_parquet(key, df)
    
    def write_series_projection_temp(
        self, dataset_id: str, series_code: str, year: int, month: int, df: pd.DataFrame
    ) -> None:
        """Write series projection to temporary location (WAL)."""
        key = self.paths.projection_series_temp_key(dataset_id, series_code, year, month)
        self._write_parquet(key, df)
    
    def move_series_projection_from_temp(
        self, dataset_id: str, series_code: str, year: int, month: int
    ) -> None:
        """Move series projection from temporary to final location (atomic)."""
        temp_key = self.paths.projection_series_temp_key(dataset_id, series_code, year, month)
        final_key = self.paths.projection_series_key(dataset_id, series_code, year, month)
        
        # Copy from temp to final
        body = self.s3.get_object(temp_key)
        self.s3.put_object(final_key, body, content_type="application/x-parquet")
        
        # Delete temp
        try:
            self.s3.s3_client.delete_object(Bucket=self.s3.bucket, Key=temp_key)
        except Exception:
            pass  # Ignore if temp doesn't exist or any error
    
    def cleanup_temp_projections(self, dataset_id: str, year: int, month: int) -> None:
        """Clean up temporary projections for a month."""
        prefix = f"datasets/{dataset_id}/projections/windows/"
        all_keys = self.s3.list_objects(prefix)
        temp_keys = [
            key for key in all_keys
            if f"year={year}/month={month:02d}/.tmp/" in key
        ]
        
        for key in temp_keys:
            try:
                self.s3.s3_client.delete_object(Bucket=self.s3.bucket, Key=key)
            except ClientError:
                pass
    
    def read_consolidation_manifest(self, dataset_id: str, year: int, month: int) -> Optional[dict]:
        """Read consolidation manifest."""
        key = self.paths.consolidation_manifest_key(dataset_id, year, month)
        return self._read_json(key)
    
    def write_consolidation_manifest(
        self, dataset_id: str, year: int, month: int, status: str
    ) -> None:
        """Write consolidation manifest."""
        key = self.paths.consolidation_manifest_key(dataset_id, year, month)
        manifest = {
            "dataset_id": dataset_id,
            "year": year,
            "month": month,
            "status": status,
            "timestamp": self.clock.now_iso(),
        }
        self._write_json(key, manifest)

