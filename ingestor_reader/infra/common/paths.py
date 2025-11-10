"""Centralized S3 path building."""
from typing import Optional


class S3PathBuilder:
    """Builder for S3 paths following consistent structure."""
    
    @staticmethod
    def config_key(dataset_id: str) -> str:
        """Get config key."""
        return f"datasets/{dataset_id}/configs/config.yaml"
    
    @staticmethod
    def index_key(dataset_id: str) -> str:
        """Get index key."""
        return f"datasets/{dataset_id}/index/keys.parquet"
    
    @staticmethod
    def current_manifest_key(dataset_id: str) -> str:
        """Get current manifest pointer key."""
        return f"datasets/{dataset_id}/current/manifest.json"
    
    @staticmethod
    def events_prefix(dataset_id: str, version_ts: str) -> str:
        """Get events prefix."""
        return f"datasets/{dataset_id}/events/{version_ts}/data/"
    
    @staticmethod
    def event_manifest_key(dataset_id: str, version_ts: str) -> str:
        """Get event manifest key."""
        return f"datasets/{dataset_id}/events/{version_ts}/manifest.json"
    
    @staticmethod
    def event_manifest_pointer(dataset_id: str, version_ts: str) -> str:
        """
        Get manifest pointer path for an event (relative path without datasets/ prefix).
        
        Returns the relative path within the bucket for use in SNS notifications.
        """
        return f"{dataset_id}/events/{version_ts}/manifest.json"
    
    @staticmethod
    def projection_series_key(dataset_id: str, series_code: str, year: int, month: int) -> str:
        """Get projection series key."""
        return f"datasets/{dataset_id}/projections/windows/{series_code}/year={year}/month={month:02d}/data.parquet"
    
    @staticmethod
    def projection_series_temp_key(dataset_id: str, series_code: str, year: int, month: int) -> str:
        """Get projection series temporary key (WAL)."""
        return f"datasets/{dataset_id}/projections/windows/{series_code}/year={year}/month={month:02d}/.tmp/data.parquet"
    
    @staticmethod
    def consolidation_manifest_key(dataset_id: str, year: int, month: int) -> str:
        """Get consolidation manifest key."""
        return f"datasets/{dataset_id}/projections/consolidation/{year}/{month:02d}/manifest.json"
    
    @staticmethod
    def event_index_key(dataset_id: str, year: int, month: int) -> str:
        """Get event index key for a month."""
        return f"datasets/{dataset_id}/events/index/{year}/{month:02d}/versions.json"
    
    @staticmethod
    def event_partition_path(year: int, month: int) -> str:
        """Get event partition path (year=YYYY/month=MM/)."""
        return f"year={year}/month={month:02d}/"
    
    @staticmethod
    def event_file_key(prefix: str, partition_path: Optional[str] = None) -> str:
        """Get event file key."""
        if partition_path:
            return f"{prefix}{partition_path}part-0.parquet"
        return f"{prefix}part-0.parquet"

