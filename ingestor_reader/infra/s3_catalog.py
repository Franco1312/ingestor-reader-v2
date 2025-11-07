"""S3 catalog path management and operations."""
import json
from typing import Optional
import pandas as pd
from botocore.exceptions import ClientError

from ingestor_reader.domain.entities.manifest import Manifest
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.parquet_io import ParquetIO


class S3Catalog:
    """S3 catalog operations."""
    
    def __init__(self, s3_storage: S3Storage):
        """
        Initialize S3 catalog.
        
        Args:
            s3_storage: S3 storage instance
        """
        self.s3 = s3_storage
        self.parquet_io = ParquetIO()
    
    def _config_key(self, dataset_id: str) -> str:
        """Get config key."""
        return f"datasets/{dataset_id}/configs/config.yaml"
    
    def _index_key(self, dataset_id: str) -> str:
        """Get index key."""
        return f"datasets/{dataset_id}/index/keys.parquet"
    
    def _version_manifest_key(self, dataset_id: str, version_ts: str) -> str:
        """Get version manifest key."""
        return f"datasets/{dataset_id}/versions/{version_ts}/manifest.json"
    
    def _current_manifest_key(self, dataset_id: str) -> str:
        """Get current manifest pointer key."""
        return f"datasets/{dataset_id}/current/manifest.json"
    
    def _run_raw_prefix(self, dataset_id: str, run_id: str) -> str:
        """Get run raw prefix."""
        return f"datasets/{dataset_id}/runs/{run_id}/raw/"
    
    def _run_staging_key(self, dataset_id: str, run_id: str) -> str:
        """Get run staging normalized key."""
        return f"datasets/{dataset_id}/runs/{run_id}/staging/normalized.parquet"
    
    def _run_delta_key(self, dataset_id: str, run_id: str) -> str:
        """Get run delta key."""
        return f"datasets/{dataset_id}/runs/{run_id}/delta/added.parquet"
    
    def _outputs_prefix(self, dataset_id: str, version_ts: str) -> str:
        """Get outputs prefix."""
        return f"datasets/{dataset_id}/outputs/{version_ts}/data/"
    
    def get_current_manifest_etag(self, dataset_id: str) -> Optional[str]:
        """Get ETag of current manifest."""
        key = self._current_manifest_key(dataset_id)
        metadata = self.s3.head_object(key)
        return metadata["ETag"] if metadata else None
    
    def read_current_manifest(self, dataset_id: str) -> Optional[dict]:
        """Read current manifest pointer."""
        key = self._current_manifest_key(dataset_id)
        try:
            body = self.s3.get_object(key)
            return json.loads(body.decode())
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                return None
            raise
        except json.JSONDecodeError:
            return None
    
    def put_current_manifest_pointer(
        self, dataset_id: str, body: dict, if_match_etag: Optional[str]
    ) -> str:
        """
        Update current manifest pointer with CAS.
        
        Args:
            dataset_id: Dataset ID
            body: Manifest pointer body
            if_match_etag: ETag for conditional PUT
            
        Returns:
            New ETag
            
        Raises:
            ValueError: If conditional check fails
        """
        key = self._current_manifest_key(dataset_id)
        body_bytes = json.dumps(body, indent=2).encode()
        return self.s3.put_object(
            key, body_bytes, content_type="application/json", if_match=if_match_etag
        )
    
    def write_manifest(self, dataset_id: str, version_ts: str, manifest: Manifest) -> None:
        """Write version manifest."""
        key = self._version_manifest_key(dataset_id, version_ts)
        body = manifest.model_dump_json(indent=2)
        self.s3.put_object(key, body.encode(), content_type="application/json")
    
    def read_index(self, dataset_id: str) -> Optional[pd.DataFrame]:
        """Read index DataFrame."""
        key = self._index_key(dataset_id)
        try:
            body = self.s3.get_object(key)
            return self.parquet_io.read_from_bytes(body)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return None
            raise
    
    def write_index(self, dataset_id: str, df: pd.DataFrame) -> None:
        """Write index DataFrame."""
        key = self._index_key(dataset_id)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def write_outputs(
        self, dataset_id: str, version_ts: str, df: pd.DataFrame
    ) -> list[str]:
        """
        Write output parquet files partitioned by year/month.
        
        Partitions data by year/month based on obs_time or obs_date column.
        Uses Hive-style partitioning: year=YYYY/month=MM/part-*.parquet
        
        Returns:
            List of written file keys
        """
        if len(df) == 0:
            return []
        
        prefix = self._outputs_prefix(dataset_id, version_ts)
        output_keys = []
        
        # Determine date column (prefer obs_time, fallback to obs_date)
        date_col = None
        if "obs_time" in df.columns:
            date_col = "obs_time"
        elif "obs_date" in df.columns:
            date_col = "obs_date"
        
        if date_col is None:
            # No date column, write single file without partitioning
            key = f"{prefix}part-0.parquet"
            body = self.parquet_io.write_to_bytes(df)
            self.s3.put_object(key, body, content_type="application/x-parquet")
            return [key]
        
        # Extract year and month from date column
        df_with_partitions = df.copy()
        if date_col == "obs_time":
            df_with_partitions["year"] = pd.to_datetime(df[date_col]).dt.year
            df_with_partitions["month"] = pd.to_datetime(df[date_col]).dt.month
        else:  # obs_date
            df_with_partitions["year"] = pd.to_datetime(df[date_col]).dt.year
            df_with_partitions["month"] = pd.to_datetime(df[date_col]).dt.month
        
        # Group by year/month and write separate files
        for (year, month), group_df in df_with_partitions.groupby(["year", "month"]):
            # Remove partition columns before writing
            group_df_clean = group_df.drop(columns=["year", "month"])
            
            # Hive-style partition path
            partition_path = f"year={year}/month={month:02d}/"
            key = f"{prefix}{partition_path}part-0.parquet"
            
            body = self.parquet_io.write_to_bytes(group_df_clean)
            self.s3.put_object(key, body, content_type="application/x-parquet")
            output_keys.append(key)
        
        return output_keys
    
    def write_run_staging(self, dataset_id: str, run_id: str, df: pd.DataFrame) -> None:
        """Write staging normalized parquet."""
        key = self._run_staging_key(dataset_id, run_id)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def write_run_delta(self, dataset_id: str, run_id: str, df: pd.DataFrame) -> None:
        """Write delta parquet."""
        key = self._run_delta_key(dataset_id, run_id)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def write_run_raw(self, dataset_id: str, run_id: str, filename: str, body: bytes) -> str:
        """Write raw file and return key."""
        prefix = self._run_raw_prefix(dataset_id, run_id)
        key = f"{prefix}{filename}"
        self.s3.put_object(key, body)
        return key

