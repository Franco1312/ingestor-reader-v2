"""S3 event store operations."""
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_stores.base import S3BaseStore
from ingestor_reader.infra.common.dataframe_utils import find_date_column, add_year_month_partitions


class S3EventStore(S3BaseStore):
    """S3 store for event operations."""
    
    def write_events(
        self, dataset_id: str, version_ts: str, df: pd.DataFrame
    ) -> list[str]:
        """
        Write event parquet files partitioned by year/month.
        
        Resilient: If any event write fails, rolls back all written events.
        If index update fails, rolls back all written events.
        """
        if len(df) == 0:
            return []
        
        prefix = self.paths.events_prefix(dataset_id, version_ts)
        date_col = find_date_column(df)
        
        if date_col is None:
            return self._write_single_event(prefix, df)
        
        return self._write_partitioned_events(dataset_id, version_ts, prefix, df, date_col)
    
    def _write_single_event(self, prefix: str, df: pd.DataFrame) -> list[str]:
        """Write single event file without partitioning."""
        key = self.paths.event_file_key(prefix)
        self._write_parquet(key, df)
        return [key]
    
    def _write_partitioned_events(
        self, dataset_id: str, version_ts: str, prefix: str, df: pd.DataFrame, date_col: str
    ) -> list[str]:
        """Write partitioned events with rollback on failure."""
        df_with_partitions = add_year_month_partitions(df, date_col)
        event_keys = []
        affected_months = set()
        
        try:
            self._write_event_files(prefix, df_with_partitions, affected_months, event_keys)
            self._update_event_indexes(dataset_id, version_ts, affected_months)
            return event_keys
        except Exception:
            self._rollback_events(event_keys)
            raise
    
    def _write_event_files(
        self, prefix: str, df_with_partitions: pd.DataFrame, affected_months: set, event_keys: list[str]
    ) -> None:
        """Write event files for each partition."""
        for (year, month), group_df in df_with_partitions.groupby(["year", "month"]):
            group_df_clean = group_df.drop(columns=["year", "month"])
            partition_path = self.paths.event_partition_path(year, month)
            key = self.paths.event_file_key(prefix, partition_path)
            
            # Write event first (may raise exception)
            self._write_parquet(key, group_df_clean)
            
            # Only add key after successful write
            event_keys.append(key)
            affected_months.add((year, month))
    
    def _update_event_indexes(
        self, dataset_id: str, version_ts: str, affected_months: set
    ) -> None:
        """Update event index for all affected months."""
        for year, month in affected_months:
            self._update_event_index(dataset_id, year, month, version_ts)
    
    def _update_event_index(
        self, dataset_id: str, year: int, month: int, version_ts: str
    ) -> None:
        """Update event index for a month by adding a new version."""
        index = self.read_event_index(dataset_id, year, month)
        versions = index.get("versions", []) if index else []
        
        if version_ts not in versions:
            versions.append(version_ts)
            self.write_event_index(dataset_id, year, month, versions)
    
    def _rollback_events(self, event_keys: list[str]) -> None:
        """Delete all written events on rollback."""
        for key in event_keys:
            try:
                self.s3.delete_object(key)
            except Exception:
                pass  # Ignore errors during rollback
    
    def list_events_for_month(self, dataset_id: str, year: int, month: int) -> list[str]:
        """
        List all event keys for a specific month using event index.
        
        Uses persistent index for fast lookup. Falls back to listing all objects
        if index doesn't exist, then creates index for future use.
        """
        # Try to read index first
        index = self.read_event_index(dataset_id, year, month)
        
        if index and "versions" in index:
            # Build keys from index (fast path)
            event_keys = [
                self.paths.event_file_key(
                    self.paths.events_prefix(dataset_id, version),
                    self.paths.event_partition_path(year, month)
                )
                for version in index["versions"]
            ]
            return sorted(event_keys)
        
        # Fallback: list all objects (slow path)
        prefix = f"datasets/{dataset_id}/events/"
        all_keys = self.s3.list_objects(prefix)
        partition_path = self.paths.event_partition_path(year, month)
        matching_keys = [
            key for key in all_keys
            if f"{partition_path}part-0.parquet" in key
        ]
        
        # Create index for future use
        if matching_keys:
            versions = [
                self._extract_version_from_key(key)
                for key in matching_keys
                if self._extract_version_from_key(key)
            ]
            if versions:
                self.write_event_index(dataset_id, year, month, versions)
        
        return sorted(matching_keys)
    
    def _extract_version_from_key(self, key: str) -> Optional[str]:
        """Extract version_ts from event key."""
        parts = key.split("/")
        for i, part in enumerate(parts):
            if part == "events" and i + 1 < len(parts):
                return parts[i + 1]
        return None
    
    def read_event_index(self, dataset_id: str, year: int, month: int) -> Optional[dict]:
        """Read event index for a month."""
        key = self.paths.event_index_key(dataset_id, year, month)
        return self._read_json(key)
    
    def write_event_index(
        self, dataset_id: str, year: int, month: int, versions: list[str]
    ) -> None:
        """Write event index for a month."""
        key = self.paths.event_index_key(dataset_id, year, month)
        index = {
            "dataset_id": dataset_id,
            "year": int(year),
            "month": int(month),
            "versions": sorted([str(v) for v in versions]),
            "last_updated": self.clock.now_iso(),
            "event_count": len(versions),
        }
        self._write_json(key, index)

