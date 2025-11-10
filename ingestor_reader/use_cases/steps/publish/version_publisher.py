"""Version publisher with CAS."""
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.manifest import SourceFile
from ingestor_reader.domain.services.delta_service import update_index
from ingestor_reader.infra.common import get_logger
from ingestor_reader.use_cases.steps.publish.manifest_builder import ManifestBuilder

logger = get_logger(__name__)


class VersionPublisher:
    """Publishes versions atomically using CAS."""
    
    def __init__(self, catalog: S3Catalog, manifest_builder: ManifestBuilder | None = None):
        """
        Initialize version publisher.
        
        Args:
            catalog: S3 catalog instance
            manifest_builder: Manifest builder instance (defaults to new instance)
        """
        self.catalog = catalog
        self.manifest_builder = manifest_builder or ManifestBuilder()
    
    def publish_version(
        self,
        dataset_id: str,
        version_ts: str,
        source_file: SourceFile,
        output_keys: list[str],
        rows_added: int,
        primary_keys: list[str],
        current_index_df: Optional[pd.DataFrame],
        delta_df: pd.DataFrame,
        current_manifest_etag: Optional[str],
    ) -> bool:
        """
        Publish version with atomic pointer update.
        
        Args:
            dataset_id: Dataset ID
            version_ts: Version timestamp
            source_file: Source file metadata
            output_keys: Output file keys
            rows_added: Number of rows added
            primary_keys: Primary key columns
            current_index_df: Current index DataFrame
            delta_df: Delta DataFrame
            current_manifest_etag: Current manifest ETag for CAS
            
        Returns:
            True if published, False if skipped (0 rows) or failed
        """
        if rows_added == 0:
            logger.info("Skipping publish: 0 rows added")
            return False
        
        logger.info("Publishing version %s", version_ts)
        
        # Update index with new rows
        updated_index_df = update_index(current_index_df, delta_df)
        
        # Calculate total rows
        total_rows = len(updated_index_df) if updated_index_df is not None else rows_added
        
        # Build manifest
        manifest = self.manifest_builder.build_manifest(
            dataset_id=dataset_id,
            version_ts=version_ts,
            source_file=source_file,
            output_keys=output_keys,
            rows_added=rows_added,
            total_rows=total_rows,
            primary_keys=primary_keys,
        )
        
        # Write event manifest
        self.catalog.write_event_manifest(dataset_id, version_ts, manifest)
        
        # Update pointer with CAS
        pointer_body = {
            "dataset_id": dataset_id,
            "current_version": version_ts,
        }
        
        try:
            # CAS update pointer
            self.catalog.put_current_manifest_pointer(dataset_id, pointer_body, current_manifest_etag)
            
            # Update index (only after successful CAS)
            self.catalog.write_index(dataset_id, updated_index_df)
            
            logger.info("Published version %s", version_ts)
            return True
        except ValueError as e:
            # CAS failed: pointer unchanged, index not updated
            logger.error("Failed to publish (CAS failed): %s. Index not updated, pointer unchanged.", e)
            return False

