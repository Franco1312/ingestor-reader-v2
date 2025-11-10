"""Generic normalizer plugin."""
import pandas as pd

from ingestor_reader.domain.plugins.base import NormalizerPlugin
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


class GenericNormalizer(NormalizerPlugin):
    """Generic normalizer for standard date/value normalization."""
    
    def __init__(self, plugin_id: str = "generic"):
        """Initialize generic normalizer with plugin ID."""
        self._plugin_id = plugin_id
    
    @property
    def id(self) -> str:
        """Get plugin ID."""
        return self._plugin_id
    
    def normalize(self, config, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame - parse dates and ensure proper types.
        
        This is a generic normalizer that handles standard date/value normalization.
        It can be used for datasets that don't require custom normalization logic.
        """
        logger.info(f"Normalizing {len(df)} rows")
        
        df = df.copy()
        
        # Parse dates
        if "obs_time" in df.columns:
            df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce")
        
        # Parse numeric values
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        
        # Apply timezone if configured
        if config.normalize.timezone and "obs_time" in df.columns:
            df["obs_time"] = df["obs_time"].dt.tz_localize(config.normalize.timezone)
        
        # Drop rows with missing required fields
        df = df.dropna(subset=["obs_time", "value"])
        
        logger.info(f"Normalized to {len(df)} rows")
        return df

