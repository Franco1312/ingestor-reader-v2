"""Write events step."""
import pandas as pd
import logging

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.dataset_config import DatasetConfig

logger = logging.getLogger(__name__)


def write_events(
    catalog: S3Catalog,
    config: DatasetConfig,
    version_ts: str,
    enriched_delta_df: pd.DataFrame,
) -> tuple[list[str], int]:
    """
    Write events to S3.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        version_ts: Version timestamp
        enriched_delta_df: Enriched delta DataFrame (with metadata, without key_hash)
        
    Returns:
        Tuple of (event file keys, total rows)
    """
    logger.info("Writing events for version %s", version_ts)
    

    if len(enriched_delta_df) > 0:
        event_keys = catalog.write_events(config.dataset_id, version_ts, enriched_delta_df)
        total_rows = len(enriched_delta_df)
    else:
        event_keys = []
        total_rows = 0
    
    logger.info("Wrote %d event files, %d rows", len(event_keys), total_rows)
    return event_keys, total_rows

