"""S3 catalog facade that composes specialized stores."""
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.s3_stores import (
    S3ManifestStore,
    S3IndexStore,
    S3EventStore,
    S3ProjectionStore,
)


class S3Catalog:
    """
    S3 catalog facade that composes specialized stores.
    
    This class provides a unified interface to all S3 operations while
    delegating to specialized stores for better organization and maintainability.
    """
    
    def __init__(self, s3_storage: S3Storage):
        """Initialize S3 catalog with specialized stores."""
        self.s3 = s3_storage
        self._manifest_store = S3ManifestStore(s3_storage)
        self._index_store = S3IndexStore(s3_storage)
        self._event_store = S3EventStore(s3_storage)
        self._projection_store = S3ProjectionStore(s3_storage)
    
    # ============================================================================
    # Manifest Operations (delegated to S3ManifestStore)
    # ============================================================================
    
    def get_current_manifest_etag(self, dataset_id: str):
        """Get ETag of current manifest."""
        return self._manifest_store.get_current_manifest_etag(dataset_id)
    
    def read_current_manifest(self, dataset_id: str):
        """Read current manifest pointer."""
        return self._manifest_store.read_current_manifest(dataset_id)
    
    def put_current_manifest_pointer(self, dataset_id: str, body: dict, if_match_etag):
        """Update current manifest pointer with CAS."""
        return self._manifest_store.put_current_manifest_pointer(dataset_id, body, if_match_etag)
    
    def write_event_manifest(self, dataset_id: str, version_ts: str, manifest):
        """Write event manifest."""
        return self._manifest_store.write_event_manifest(dataset_id, version_ts, manifest)
    
    def read_event_manifest(self, dataset_id: str, version_ts: str):
        """Read event manifest."""
        return self._manifest_store.read_event_manifest(dataset_id, version_ts)
    
    def get_event_manifest_pointer(self, dataset_id: str, version_ts: str) -> str:
        """Get manifest pointer path for an event (for SNS notifications)."""
        return self._manifest_store.get_event_manifest_pointer(dataset_id, version_ts)
    
    # ============================================================================
    # Index Operations (delegated to S3IndexStore)
    # ============================================================================
    
    def read_index(self, dataset_id: str):
        """Read index DataFrame."""
        return self._index_store.read_index(dataset_id)
    
    def write_index(self, dataset_id: str, df):
        """Write index DataFrame."""
        return self._index_store.write_index(dataset_id, df)
    
    def verify_pointer_index_consistency(self, dataset_id: str) -> bool:
        """Verify consistency between pointer and index."""
        return self._index_store.verify_pointer_index_consistency(dataset_id, self._manifest_store)
    
    def rebuild_index_from_pointer(self, dataset_id: str) -> None:
        """Rebuild index from pointer by reading all events up to current version."""
        return self._index_store.rebuild_index_from_pointer(dataset_id, self._manifest_store, self._event_store)
    
    # ============================================================================
    # Event Operations (delegated to S3EventStore)
    # ============================================================================
    
    def write_events(self, dataset_id: str, version_ts: str, df):
        """Write event parquet files partitioned by year/month."""
        return self._event_store.write_events(dataset_id, version_ts, df)
    
    def list_events_for_month(self, dataset_id: str, year: int, month: int) -> list[str]:
        """List all event keys for a specific month using event index."""
        return self._event_store.list_events_for_month(dataset_id, year, month)
    
    def read_event_index(self, dataset_id: str, year: int, month: int):
        """Read event index for a month."""
        return self._event_store.read_event_index(dataset_id, year, month)
    
    def write_event_index(self, dataset_id: str, year: int, month: int, versions: list[str]) -> None:
        """Write event index for a month."""
        return self._event_store.write_event_index(dataset_id, year, month, versions)
    
    # Expose internal methods for testing (backward compatibility)
    def _update_event_index(self, dataset_id: str, year: int, month: int, version_ts: str) -> None:
        """Update event index for a month (internal, exposed for testing)."""
        return self._event_store._update_event_index(dataset_id, year, month, version_ts)
    
    def _write_event_files(self, prefix: str, df_with_partitions, affected_months: set, event_keys: list[str]) -> None:
        """Write event files for each partition (internal, exposed for testing)."""
        return self._event_store._write_event_files(prefix, df_with_partitions, affected_months, event_keys)
    
    def _rollback_events(self, event_keys: list[str]) -> None:
        """Delete all written events on rollback (internal, exposed for testing)."""
        return self._event_store._rollback_events(event_keys)
    
    # ============================================================================
    # Projection Operations (delegated to S3ProjectionStore)
    # ============================================================================
    
    def read_series_projection(self, dataset_id: str, series_code: str, year: int, month: int):
        """Read series projection."""
        return self._projection_store.read_series_projection(dataset_id, series_code, year, month)
    
    def write_series_projection(self, dataset_id: str, series_code: str, year: int, month: int, df):
        """Write series projection."""
        return self._projection_store.write_series_projection(dataset_id, series_code, year, month, df)
    
    def write_series_projection_temp(self, dataset_id: str, series_code: str, year: int, month: int, df):
        """Write series projection to temporary location (WAL)."""
        return self._projection_store.write_series_projection_temp(dataset_id, series_code, year, month, df)
    
    def move_series_projection_from_temp(self, dataset_id: str, series_code: str, year: int, month: int):
        """Move series projection from temporary to final location (atomic)."""
        return self._projection_store.move_series_projection_from_temp(dataset_id, series_code, year, month)
    
    def cleanup_temp_projections(self, dataset_id: str, year: int, month: int) -> None:
        """Clean up temporary projections for a month."""
        return self._projection_store.cleanup_temp_projections(dataset_id, year, month)
    
    def read_consolidation_manifest(self, dataset_id: str, year: int, month: int):
        """Read consolidation manifest."""
        return self._projection_store.read_consolidation_manifest(dataset_id, year, month)
    
    def write_consolidation_manifest(self, dataset_id: str, year: int, month: int, status: str) -> None:
        """Write consolidation manifest."""
        return self._projection_store.write_consolidation_manifest(dataset_id, year, month, status)
    
    # ============================================================================
    # Compatibility: Expose stores for direct access if needed
    # ============================================================================
    
    @property
    def parquet_io(self):
        """Expose parquet_io for backward compatibility."""
        return self._event_store.parquet_io
    
    @property
    def paths(self):
        """Expose paths for backward compatibility."""
        return self._event_store.paths
