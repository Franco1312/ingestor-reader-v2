"""Normalize rows step."""
import pandas as pd

from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.infra.plugins.registry import get_normalizer
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


def normalize_rows(
    df: pd.DataFrame,
    config: DatasetConfig,
) -> pd.DataFrame:
    """
    Normalize rows according to config using plugin.
    
    Args:
        df: Raw parsed DataFrame
        config: Dataset configuration
        
    Returns:
        Normalized DataFrame
    """
    plugin_id = getattr(config.normalize, "plugin", None)
    normalizer = get_normalizer(plugin_id)
    
    logger.info(f"Normalizing with plugin: {normalizer.id}")
    
    df = normalizer.normalize(config, df)
    
    return df

