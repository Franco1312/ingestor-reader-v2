"""Plugin base interfaces."""
import pandas as pd
from abc import ABC, abstractmethod


class ParserPlugin(ABC):
    """Parser plugin interface."""
    
    id: str
    
    @abstractmethod
    def parse(self, config, raw_bytes: bytes) -> pd.DataFrame:
        """
        Parse raw bytes into DataFrame.
        
        Args:
            config: Dataset configuration
            raw_bytes: Raw file content
            
        Returns:
            Parsed DataFrame
        """
        raise NotImplementedError


class NormalizerPlugin(ABC):
    """Normalizer plugin interface."""
    
    id: str
    
    @abstractmethod
    def normalize(self, config, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame.
        
        Args:
            config: Dataset configuration
            df: Input DataFrame
            
        Returns:
            Normalized DataFrame
        """
        raise NotImplementedError

