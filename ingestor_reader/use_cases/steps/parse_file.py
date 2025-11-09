"""Parse file step."""
import pandas as pd
import logging

from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.infra.plugins.registry import get_parser

logger = logging.getLogger(__name__)


def parse_file(
    raw_bytes: bytes,
    config: DatasetConfig,
) -> pd.DataFrame:
    """
    Parse file according to config using plugin.
    
    Args:
        raw_bytes: Raw file content
        config: Dataset configuration
        
    Returns:
        Parsed DataFrame
    """

    plugin_id = getattr(config, "plugin", None) or getattr(config.parse, "plugin", None)
    parser = get_parser(plugin_id, config)
    
    logger.info(f"Parsing with plugin: {parser.id}")
    
    df = parser.parse(config, raw_bytes)
    
    logger.info(f"Parsed {len(df)} rows, {len(df.columns)} columns")
    return df

