"""Shared utilities for pipeline steps."""
from typing import Optional
import pandas as pd


def find_date_column(df: pd.DataFrame) -> Optional[str]:
    """
    Find date column in DataFrame (obs_time or obs_date).
    
    Args:
        df: DataFrame to search
        
    Returns:
        Column name or None if not found
    """
    if "obs_time" in df.columns:
        return "obs_time"
    if "obs_date" in df.columns:
        return "obs_date"
    return None

