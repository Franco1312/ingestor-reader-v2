"""INDEC IPC normalizer."""
import pandas as pd
import logging

from ingestor_reader.domain.plugins.base import NormalizerPlugin

logger = logging.getLogger(__name__)


class NormalizerINDECIPC(NormalizerPlugin):
    """INDEC IPC normalizer."""
    
    id = "indec_ipc"
    
    def normalize(self, config, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame - parse dates and ensure proper types."""
        logger.info(f"Normalizing {len(df)} rows")
        
        df = df.copy()
        

        if "obs_time" in df.columns:
            df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce")
        

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        

        if config.normalize.timezone and "obs_time" in df.columns:
            df["obs_time"] = df["obs_time"].dt.tz_localize(config.normalize.timezone)
        

        df = df.dropna(subset=["obs_time", "value"])
        
        logger.info(f"Normalized to {len(df)} rows")
        return df

