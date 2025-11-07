"""Compute delta step."""
import pandas as pd
import logging

from ingestor_reader.domain.services.delta_service import compute_delta

logger = logging.getLogger(__name__)


def compute_delta_step(
    normalized_df: pd.DataFrame,
    index_df: pd.DataFrame | None,
    primary_keys: list[str],
) -> pd.DataFrame:
    """
    Compute delta: new rows not in index.
    
    Args:
        normalized_df: Normalized DataFrame
        index_df: Current index (None for first run)
        primary_keys: Primary key columns
        
    Returns:
        DataFrame with only new rows
    """
    logger.info(f"Computing delta from {len(normalized_df)} normalized rows")
    
    added_df = compute_delta(normalized_df, index_df, primary_keys)
    
    logger.info(f"Delta: {len(added_df)} new rows")
    return added_df

