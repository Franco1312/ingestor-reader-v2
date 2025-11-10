"""S3 index store operations."""
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_stores.base import S3BaseStore


class S3IndexStore(S3BaseStore):
    """S3 store for index operations."""
    
    def read_index(self, dataset_id: str) -> Optional[pd.DataFrame]:
        """Read index DataFrame."""
        key = self.paths.index_key(dataset_id)
        return self._read_parquet(key)
    
    def write_index(self, dataset_id: str, df: pd.DataFrame) -> None:
        """Write index DataFrame."""
        key = self.paths.index_key(dataset_id)
        self._write_parquet(key, df)
    
    def verify_pointer_index_consistency(self, dataset_id: str, manifest_store) -> bool:
        """Verify consistency between pointer and index."""
        pointer = manifest_store.read_current_manifest(dataset_id)
        if pointer is None:
            index_df = self.read_index(dataset_id)
            return index_df is None or len(index_df) == 0
        
        current_version = pointer.get("current_version")
        if current_version is None:
            return False
        
        manifest = manifest_store.read_event_manifest(dataset_id, current_version)
        if manifest is None:
            return False
        
        index_df = self.read_index(dataset_id)
        if index_df is None:
            return False
        
        expected_rows = manifest.get("outputs", {}).get("rows_total") if isinstance(manifest, dict) else None
        if expected_rows is not None:
            actual_rows = len(index_df)
            return abs(actual_rows - expected_rows) <= 10
        
        return True
    
    def rebuild_index_from_pointer(self, dataset_id: str, manifest_store, event_store) -> None:
        """Rebuild index from pointer by reading all events up to current version."""
        pointer = manifest_store.read_current_manifest(dataset_id)
        if pointer is None:
            return
        
        current_version = pointer.get("current_version")
        if current_version is None:
            return
        
        manifest = manifest_store.read_event_manifest(dataset_id, current_version)
        if manifest is None:
            return
        
        primary_keys = manifest.get("index", {}).get("key_columns") if isinstance(manifest, dict) else None
        if primary_keys is None:
            return
        
        # Collect all events from all versions up to current
        all_events = []
        prefix = f"datasets/{dataset_id}/events/"
        all_keys = self.s3.list_objects(prefix)
        
        # Extract and sort version timestamps
        version_timestamps = set()
        for key in all_keys:
            parts = key.split("/")
            if len(parts) >= 4 and parts[2] == "events":
                version_timestamps.add(parts[3])
        
        sorted_versions = sorted(version_timestamps)
        
        # Read events from all versions up to current
        for version_ts in sorted_versions:
            if version_ts > current_version:
                break
            
            version_prefix = self.paths.events_prefix(dataset_id, version_ts)
            for key in all_keys:
                if key.startswith(version_prefix) and key.endswith(".parquet"):
                    df = self._read_parquet(key)
                    if df is not None and len(df) > 0:
                        all_events.append(df)
        
        if not all_events:
            return
        
        # Concatenate and compute key_hash
        combined_df = pd.concat(all_events, ignore_index=True)
        from ingestor_reader.domain.services.delta_service import compute_key_hash
        combined_df["key_hash"] = combined_df.apply(
            lambda row: compute_key_hash(row, primary_keys), axis=1
        )
        
        # Create index with unique key_hashes
        index_df = combined_df[["key_hash"]].drop_duplicates(subset=["key_hash"], keep="first")
        self.write_index(dataset_id, index_df)

