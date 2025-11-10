"""Series code resolution utilities."""
import pandas as pd
from typing import Optional

from ingestor_reader.domain.entities.dataset_config import DatasetConfig


def resolve_series_code(df: pd.DataFrame, config: DatasetConfig) -> pd.DataFrame:
    """
    Resolve series_code for DataFrame rows.
    
    If internal_series_code is present, uses it. Otherwise, falls back to
    dataset_id as default series_code.
    
    Args:
        df: DataFrame to process
        config: Dataset configuration
        
    Returns:
        DataFrame with internal_series_code column (added if missing)
    """
    if "internal_series_code" in df.columns:
        return df.copy()
    
    # Fallback: use dataset_id as default series_code
    df = df.copy()
    df["internal_series_code"] = config.dataset_id
    
    return df


def get_series_code_column(df: pd.DataFrame) -> Optional[str]:
    """
    Get the series code column name from DataFrame.
    
    Args:
        df: DataFrame to check
        
    Returns:
        Column name if found, None otherwise
    """
    if "internal_series_code" in df.columns:
        return "internal_series_code"
    return None

