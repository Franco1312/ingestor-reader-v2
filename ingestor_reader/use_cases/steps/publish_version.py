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


def _build_manifest(
    dataset_id: str,
    version_ts: str,
    source_file: SourceFile,
    output_keys: list[str],
    rows_added: int,
    total_rows: int,
    primary_keys: list[str],
) -> Manifest:
    """
    Build manifest for a version.
    
    Args:
        dataset_id: Dataset ID
        version_ts: Version timestamp
        source_file: Source file metadata
        output_keys: Output file keys
        rows_added: Number of rows added in this version
        total_rows: Total rows after this version
        primary_keys: Primary key columns
        
    Returns:
        Manifest object
    """
    created_at = datetime.now(timezone.utc).isoformat()
    

    source_file_dict = source_file.model_dump()
    if source_file_dict.get("path") is None:
        source_file_dict.pop("path", None)
    
    return Manifest(
        dataset_id=dataset_id,
        version=version_ts,
        created_at=created_at,
        source={
            "files": [source_file_dict]
        },
        outputs=OutputsInfo(
            data_prefix=f"datasets/{dataset_id}/events/{version_ts}/data/",
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
    
    logger.info("Publishing version %s", version_ts)
    

    updated_index_df = update_index(current_index_df, delta_df)
    

    total_rows = len(updated_index_df) if updated_index_df is not None else rows_added
    

    manifest = _build_manifest(
        dataset_id=dataset_id,
        version_ts=version_ts,
        source_file=source_file,
        output_keys=output_keys,
        rows_added=rows_added,
        total_rows=total_rows,
        primary_keys=primary_keys,
    )
    

    catalog.write_event_manifest(dataset_id, version_ts, manifest)
    

    pointer_body = {
        "dataset_id": dataset_id,
        "current_version": version_ts,
    }
    
    try:

        catalog.put_current_manifest_pointer(dataset_id, pointer_body, current_manifest_etag)
        


        catalog.write_index(dataset_id, updated_index_df)
        
        logger.info("Published version %s", version_ts)
        return True
    except ValueError as e:



        logger.error("Failed to publish (CAS failed): %s. Index not updated, pointer unchanged.", e)
        return False

