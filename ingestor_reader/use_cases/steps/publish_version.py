"""Publish version step."""
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.manifest import SourceFile
from ingestor_reader.use_cases.steps.publish import VersionPublisher


def publish_version(
    catalog: S3Catalog,
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
        catalog: S3 catalog instance
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
    publisher = VersionPublisher(catalog)
    return publisher.publish_version(
        dataset_id=dataset_id,
        version_ts=version_ts,
        source_file=source_file,
        output_keys=output_keys,
        rows_added=rows_added,
        primary_keys=primary_keys,
        current_index_df=current_index_df,
        delta_df=delta_df,
        current_manifest_etag=current_manifest_etag,
    )
