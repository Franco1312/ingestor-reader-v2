"""Common DataFrame utilities."""
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


def add_year_month_partitions(
    df: pd.DataFrame,
    date_col: str,
    drop_invalid: bool = False,
) -> pd.DataFrame:
    """
    Add year and month columns from date column.
    
    Args:
        df: DataFrame with date column
        date_col: Name of date column
        drop_invalid: If True, drop rows with invalid dates
        
    Returns:
        DataFrame with year and month columns added
    """
    df_with_partitions = df.copy()
    df_with_partitions["year"] = pd.to_datetime(df[date_col], errors="coerce").dt.year
    df_with_partitions["month"] = pd.to_datetime(df[date_col], errors="coerce").dt.month
    
    if drop_invalid:
        return df_with_partitions.dropna(subset=["year", "month"])
    
    return df_with_partitions

