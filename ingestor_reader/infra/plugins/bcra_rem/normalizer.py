"""BCRA REM normalizer."""
import pandas as pd
import logging

from ingestor_reader.domain.plugins.base import NormalizerPlugin

logger = logging.getLogger(__name__)


class NormalizerBCRAREM(NormalizerPlugin):
    """BCRA REM normalizer."""
    
    id = "bcra_rem"
    
    def normalize(self, config, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame - parse dates and ensure proper types."""
        logger.info(f"Normalizing {len(df)} rows")
        
        df = df.copy()
        
        # Parse obs_time as datetime
        if "obs_time" in df.columns:
            df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce")
        
        # Ensure value is numeric
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        
        # Apply timezone if specified
        if config.normalize.timezone and "obs_time" in df.columns:
            df["obs_time"] = df["obs_time"].dt.tz_localize(config.normalize.timezone)
        
        # Drop rows with null obs_time or value
        df = df.dropna(subset=["obs_time", "value"])
        
        logger.info(f"Normalized to {len(df)} rows")
        return df

