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
    
    @staticmethod
    def _is_not_found_error(error: ClientError) -> bool:
        """
        Check if ClientError is a 404/NoSuchKey error.
        
        Args:
            error: ClientError exception
            
        Returns:
            True if error is 404/NoSuchKey, False otherwise
        """
        error_code = error.response.get("Error", {}).get("Code", "")
        return error_code in ("404", "NoSuchKey")
    
    def _read_json(self, key: str) -> Optional[dict]:
        """
        Read JSON object from S3 with error handling.
        
        Args:
            key: S3 key
            
        Returns:
            Dict or None if not found
        """
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
        """
        Read Parquet object from S3 with error handling.
        
        Args:
            key: S3 key
            
        Returns:
            DataFrame or None if not found
        """
        try:
            body = self.s3.get_object(key)
            return self.parquet_io.read_from_bytes(body)
        except ClientError as e:
            if self._is_not_found_error(e):
                return None
            raise
    
    def _config_key(self, dataset_id: str) -> str:
        """Get config key."""
        return f"datasets-test/{dataset_id}/configs/config.yaml"
    
    def _index_key(self, dataset_id: str) -> str:
        """Get index key."""
        return f"datasets-test/{dataset_id}/index/keys.parquet"
    
    def _current_manifest_key(self, dataset_id: str) -> str:
        """Get current manifest pointer key."""
        return f"datasets-test/{dataset_id}/current/manifest.json"
    
    def _events_prefix(self, dataset_id: str, version_ts: str) -> str:
        """Get events prefix."""
        return f"datasets-test/{dataset_id}/events/{version_ts}/data/"
    
    def _event_manifest_key(self, dataset_id: str, version_ts: str) -> str:
        """Get event manifest key."""
        return f"datasets-test/{dataset_id}/events/{version_ts}/manifest.json"
    
    def _projection_series_key(self, dataset_id: str, series_code: str, year: int, month: int) -> str:
        """Get projection series key."""
        return f"datasets-test/{dataset_id}/projections/windows/{series_code}/year={year}/month={month:02d}/data.parquet"
    
    def _projection_series_temp_key(self, dataset_id: str, series_code: str, year: int, month: int) -> str:
        """Get projection series temporary key (WAL)."""
        return f"datasets-test/{dataset_id}/projections/windows/{series_code}/year={year}/month={month:02d}/.tmp/data.parquet"
    
    def _consolidation_manifest_key(self, dataset_id: str, year: int, month: int) -> str:
        """Get consolidation manifest key."""
        return f"datasets-test/{dataset_id}/projections/consolidation/{year}/{month:02d}/manifest.json"
    
    def get_current_manifest_etag(self, dataset_id: str) -> Optional[str]:
        """Get ETag of current manifest."""
        key = self._current_manifest_key(dataset_id)
        metadata = self.s3.head_object(key)
        return metadata["ETag"] if metadata else None
    
    def read_current_manifest(self, dataset_id: str) -> Optional[dict]:
        """Read current manifest pointer."""
        key = self._current_manifest_key(dataset_id)
        return self._read_json(key)
    
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
        key = self._event_manifest_key(dataset_id, version_ts)
        body = manifest.model_dump_json(indent=2)
        self.s3.put_object(key, body.encode(), content_type="application/json")
    
    def read_event_manifest(self, dataset_id: str, version_ts: str) -> Optional[dict]:
        """
        Read event manifest.
        
        Args:
            dataset_id: Dataset ID
            version_ts: Version timestamp
            
        Returns:
            Manifest dict or None if not found
        """
        key = self._event_manifest_key(dataset_id, version_ts)
        return self._read_json(key)
    
    def get_event_manifest_pointer(self, dataset_id: str, version_ts: str) -> str:
        """
        Get manifest pointer path for an event.
        
        Returns the relative path within the bucket (without the "datasets/" prefix)
        for use in SNS notifications.
        
        Args:
            dataset_id: Dataset ID
            version_ts: Version timestamp
            
        Returns:
            Manifest pointer path (relative path within bucket, without "datasets/" prefix)
        """
        return f"{dataset_id}/events/{version_ts}/manifest.json"
    
    def read_index(self, dataset_id: str) -> Optional[pd.DataFrame]:
        """Read index DataFrame."""
        key = self._index_key(dataset_id)
        return self._read_parquet(key)
    
    def write_index(self, dataset_id: str, df: pd.DataFrame) -> None:
        """Write index DataFrame."""
        key = self._index_key(dataset_id)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def _find_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Find date column in DataFrame (obs_time or obs_date).
        
        Args:
            df: DataFrame to search
            
        Returns:
            Column name or None if not found
        """
        if "obs_time" in df.columns:
            return "obs_time"
        if "obs_date" in df.columns:
            return "obs_date"
        return None
    
    def _add_year_month_partitions(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Add year and month columns for partitioning.
        
        Args:
            df: DataFrame with date column
            date_col: Name of date column
            
        Returns:
            DataFrame with year and month columns added
        """
        df_with_partitions = df.copy()
        df_with_partitions["year"] = pd.to_datetime(df[date_col]).dt.year
        df_with_partitions["month"] = pd.to_datetime(df[date_col]).dt.month
        return df_with_partitions
    
    def write_events(
        self, dataset_id: str, version_ts: str, df: pd.DataFrame
    ) -> list[str]:
        """
        Write event parquet files partitioned by year/month.
        
        Partitions data by year/month based on obs_time or obs_date column.
        Uses Hive-style partitioning: year=YYYY/month=MM/part-*.parquet
        
        Returns:
            List of written file keys
        """
        if len(df) == 0:
            return []
        
        prefix = self._events_prefix(dataset_id, version_ts)
        date_col = self._find_date_column(df)
        
        if date_col is None:
            key = f"{prefix}part-0.parquet"
            body = self.parquet_io.write_to_bytes(df)
            self.s3.put_object(key, body, content_type="application/x-parquet")
            return [key]
        
        df_with_partitions = self._add_year_month_partitions(df, date_col)
        event_keys = []
        
        for (year, month), group_df in df_with_partitions.groupby(["year", "month"]):
            group_df_clean = group_df.drop(columns=["year", "month"])
            partition_path = f"year={year}/month={month:02d}/"
            key = f"{prefix}{partition_path}part-0.parquet"
            
            body = self.parquet_io.write_to_bytes(group_df_clean)
            self.s3.put_object(key, body, content_type="application/x-parquet")
            event_keys.append(key)
        
        return event_keys
    
    def list_events_for_month(self, dataset_id: str, year: int, month: int) -> list[str]:
        """
        List all event keys for a specific month.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
            
        Returns:
            List of event keys for the month
        """
        prefix = f"datasets-test/{dataset_id}/events/"
        
        all_keys = self.s3.list_objects(prefix)
        matching_keys = [
            key for key in all_keys
            if f"year={year}/month={month:02d}/part-0.parquet" in key
        ]
        
        return sorted(matching_keys)
    
    def read_series_projection(
        self, dataset_id: str, series_code: str, year: int, month: int
    ) -> Optional[pd.DataFrame]:
        """
        Read series projection.
        
        Args:
            dataset_id: Dataset ID
            series_code: Series code
            year: Year
            month: Month (1-12)
            
        Returns:
            DataFrame or None if not found
        """
        key = self._projection_series_key(dataset_id, series_code, year, month)
        return self._read_parquet(key)
    
    def write_series_projection(
        self, dataset_id: str, series_code: str, year: int, month: int, df: pd.DataFrame
    ) -> None:
        """
        Write series projection.
        
        Args:
            dataset_id: Dataset ID
            series_code: Series code
            year: Year
            month: Month (1-12)
            df: DataFrame to write
        """
        key = self._projection_series_key(dataset_id, series_code, year, month)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def write_series_projection_temp(
        self, dataset_id: str, series_code: str, year: int, month: int, df: pd.DataFrame
    ) -> None:
        """
        Write series projection to temporary location (WAL).
        
        Args:
            dataset_id: Dataset ID
            series_code: Series code
            year: Year
            month: Month (1-12)
            df: DataFrame to write
        """
        key = self._projection_series_temp_key(dataset_id, series_code, year, month)
        body = self.parquet_io.write_to_bytes(df)
        self.s3.put_object(key, body, content_type="application/x-parquet")
    
    def move_series_projection_from_temp(
        self, dataset_id: str, series_code: str, year: int, month: int
    ) -> None:
        """
        Move series projection from temporary to final location (atomic).
        
        Args:
            dataset_id: Dataset ID
            series_code: Series code
            year: Year
            month: Month (1-12)
        """
        temp_key = self._projection_series_temp_key(dataset_id, series_code, year, month)
        final_key = self._projection_series_key(dataset_id, series_code, year, month)
        
        # Copy from temp to final
        body = self.s3.get_object(temp_key)
        self.s3.put_object(final_key, body, content_type="application/x-parquet")
        
        # Delete temp
        try:
            self.s3.s3_client.delete_object(Bucket=self.s3.bucket, Key=temp_key)
        except (ClientError, Exception):
            pass  # Ignore if temp doesn't exist or any error
    
    def cleanup_temp_projections(self, dataset_id: str, year: int, month: int) -> None:
        """
        Clean up temporary projections for a month.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
        """
        prefix = f"datasets-test/{dataset_id}/projections/windows/"
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
        """
        Read consolidation manifest.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
            
        Returns:
            Manifest dict or None if not found
        """
        key = self._consolidation_manifest_key(dataset_id, year, month)
        return self._read_json(key)
    
    def write_consolidation_manifest(
        self, dataset_id: str, year: int, month: int, status: str
    ) -> None:
        """
        Write consolidation manifest.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
            status: Status ("in_progress" or "completed")
        """
        from datetime import datetime, timezone
        
        key = self._consolidation_manifest_key(dataset_id, year, month)
        manifest = {
            "dataset_id": dataset_id,
            "year": year,
            "month": month,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        body = json.dumps(manifest, indent=2).encode()
        self.s3.put_object(key, body, content_type="application/json")
    

