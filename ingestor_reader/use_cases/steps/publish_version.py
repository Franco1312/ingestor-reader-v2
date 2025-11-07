"""Publish version step."""
from datetime import datetime, timezone
from typing import Optional
import logging
import pandas as pd

from ingestor_reader.domain.entities.manifest import (
    Manifest,
    SourceFile,
    OutputsInfo,
    IndexInfo,
)
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.services.delta_service import update_index

logger = logging.getLogger(__name__)


def publish_version(
    catalog: S3Catalog,
    dataset_id: str,
    version_ts: str,
    source_file: SourceFile,
    output_keys: list[str],
    rows_added: int,
    primary_keys: list[str],
    lag_days: int,
    current_index_df: Optional[pd.DataFrame],
    delta_df: pd.DataFrame,
    current_manifest_etag: Optional[str],
) -> bool:
    """
    Publish version with atomic pointer update.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        version_ts: Version timestamp
        source_file: Source file metadata
        output_keys: Output file keys
        rows_added: Number of rows added
        primary_keys: Primary key columns
        lag_days: Lag days
        current_index_df: Current index DataFrame
        delta_df: Delta DataFrame
        current_manifest_etag: Current manifest ETag for CAS
        
    Returns:
        True if published, False if skipped (0 rows) or failed
    """
    if rows_added == 0:
        logger.info("Skipping publish: 0 rows added")
        return False
    
    logger.info(f"Publishing version {version_ts}")
    
    # Prepare updated index (but don't write yet - only after successful CAS)
    updated_index_df = update_index(current_index_df, delta_df)
    
    # Build manifest
    created_at = datetime.now(timezone.utc).isoformat()
    
    # Compute total rows (current + added)
    total_rows = len(updated_index_df) if updated_index_df is not None else rows_added
    
    manifest = Manifest(
        dataset_id=dataset_id,
        version=version_ts,
        created_at=created_at,
        source={
            "files": [source_file.model_dump()]
        },
        outputs=OutputsInfo(
            data_prefix=f"datasets/{dataset_id}/outputs/{version_ts}/data/",
            files=output_keys,
            rows_total=total_rows,
            rows_added_this_version=rows_added,
        ),
        index=IndexInfo(
            path=f"datasets/{dataset_id}/index/keys.parquet",
            key_columns=primary_keys,
            hash_column="key_hash",
        ),
    )
    
    # Write version manifest (safe - it's under versions/<version_ts>/)
    catalog.write_manifest(dataset_id, version_ts, manifest)
    
    # Atomic pointer update with CAS
    pointer_body = {
        "dataset_id": dataset_id,
        "current_version": version_ts,
    }
    
    try:
        # Try CAS update - this is the critical atomic operation
        catalog.put_current_manifest_pointer(dataset_id, pointer_body, current_manifest_etag)
        
        # Only update index AFTER successful pointer update
        # This ensures consistency: if CAS fails, index stays unchanged
        catalog.write_index(dataset_id, updated_index_df)
        
        logger.info(f"Published version {version_ts}")
        return True
    except ValueError as e:
        # CAS failed - rollback: don't update index, pointer unchanged
        # Outputs and manifest remain under versions/<version_ts>/ but are not visible
        # (pointer doesn't point to them)
        logger.error(f"Failed to publish (CAS failed): {e}. Index not updated, pointer unchanged.")
        return False

