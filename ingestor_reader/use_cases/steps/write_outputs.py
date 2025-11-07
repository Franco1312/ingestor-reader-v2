"""Write outputs step."""
import pandas as pd
import logging

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.dataset_config import DatasetConfig

logger = logging.getLogger(__name__)


def write_outputs(
    catalog: S3Catalog,
    config: DatasetConfig,
    run_id: str,
    version_ts: str,
    normalized_df: pd.DataFrame,
    enriched_delta_df: pd.DataFrame,
    delta_df_with_hash: pd.DataFrame | None = None,
) -> tuple[list[str], int]:
    """
    Write outputs to S3.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        run_id: Run ID
        version_ts: Version timestamp
        normalized_df: Normalized DataFrame
        enriched_delta_df: Enriched delta DataFrame (with metadata, without key_hash)
        delta_df_with_hash: Optional delta DataFrame with key_hash for debugging
        
    Returns:
        Tuple of (output file keys, total rows)
    """
    logger.info(f"Writing outputs for version {version_ts}")
    
    # Write staging normalized
    catalog.write_run_staging(config.dataset_id, run_id, normalized_df)
    
    # Write delta (with key_hash for debugging) if provided
    if delta_df_with_hash is not None and len(delta_df_with_hash) > 0:
        catalog.write_run_delta(config.dataset_id, run_id, delta_df_with_hash)
    
    # Write outputs (only delta for incremental)
    if len(enriched_delta_df) > 0:
        output_keys = catalog.write_outputs(config.dataset_id, version_ts, enriched_delta_df)
        total_rows = len(enriched_delta_df)
    else:
        output_keys = []
        total_rows = 0
    
    logger.info(f"Wrote {len(output_keys)} output files, {total_rows} rows")
    return output_keys, total_rows

