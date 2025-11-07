"""Filter new data by date step."""
import json
import logging
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.parquet_io import ParquetIO

logger = logging.getLogger(__name__)


def _find_date_column(df: pd.DataFrame) -> Optional[str]:
    """Find date column in DataFrame (obs_time or obs_date)."""
    if "obs_time" in df.columns:
        return "obs_time"
    if "obs_date" in df.columns:
        return "obs_date"
    return None


def _get_max_date_from_file(catalog: S3Catalog, file_key: str) -> Optional[pd.Timestamp]:
    """Get maximum date from a single output file."""
    try:
        parquet_body = catalog.s3.get_object(file_key)
        parquet_io = ParquetIO()
        df = parquet_io.read_from_bytes(parquet_body)
        
        date_column = _find_date_column(df)
        if not date_column:
            return None
        
        df[date_column] = pd.to_datetime(df[date_column])
        return df[date_column].max()
    except (KeyError, ValueError, AttributeError) as e:
        logger.warning("Could not read output file %s: %s", file_key, e)
        return None


def _get_last_version_manifest(catalog: S3Catalog, dataset_id: str) -> Optional[dict]:
    """Get last version manifest from catalog."""
    current_manifest = catalog.read_current_manifest(dataset_id)
    if current_manifest is None:
        return None
    
    last_version = current_manifest.get("current_version")
    if not last_version:
        return None
    
    return catalog.read_version_manifest(dataset_id, last_version)


def _get_latest_date_from_outputs(catalog: S3Catalog, dataset_id: str) -> Optional[pd.Timestamp]:
    """Get latest date from published outputs."""
    manifest = _get_last_version_manifest(catalog, dataset_id)
    if manifest is None:
        return None
    
    output_files = manifest.get("outputs", {}).get("files", [])
    if not output_files:
        return None
    
    latest_date = None
    for file_key in output_files:
        max_date = _get_max_date_from_file(catalog, file_key)
        if max_date is not None:
            if latest_date is None or max_date > latest_date:
                latest_date = max_date
    
    return latest_date


def _normalize_timezones(
    df: pd.DataFrame,
    date_column: str,
    cutoff_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Normalize timezones between DataFrame and cutoff date for comparison."""
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    
    df_tz = df[date_column].dt.tz
    cutoff_tz = cutoff_date.tz
    
    # If DataFrame is naive and cutoff is aware, remove timezone from cutoff
    if df_tz is None and cutoff_tz is not None:
        cutoff_date = cutoff_date.tz_localize(None)
    # If DataFrame is aware and cutoff is naive, remove timezone from DataFrame
    elif df_tz is not None and cutoff_tz is None:
        df[date_column] = df[date_column].dt.tz_localize(None)
    
    return df, cutoff_date


def filter_new_data(
    catalog: S3Catalog,
    dataset_id: str,
    parsed_df: pd.DataFrame,
    date_column: str = "obs_time",
) -> pd.DataFrame:
    """
    Filter parsed data to only include new rows (after last processed date).
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        parsed_df: Parsed DataFrame
        date_column: Name of date column (obs_time or obs_date)
        
    Returns:
        DataFrame with only new data (after last processed date)
    """
    # Get latest date from published outputs
    latest_date = _get_latest_date_from_outputs(catalog, dataset_id)
    
    if latest_date is None:
        logger.info("No previous data found, processing all rows")
        return parsed_df
    
    # Validate date column exists
    if date_column not in parsed_df.columns:
        logger.warning("Date column %s not found, processing all rows", date_column)
        return parsed_df
    
    # Normalize timezones for comparison
    normalized_df, normalized_cutoff = _normalize_timezones(parsed_df, date_column, latest_date)
    
    # Filter: only rows with date > cutoff
    new_data = normalized_df[normalized_df[date_column] > normalized_cutoff].copy()
    
    total_rows = len(normalized_df)
    new_rows = len(new_data)
    skipped_rows = total_rows - new_rows
    
    logger.info(
        "Filtered data: %d rows total, %d new (after %s), %d skipped",
        total_rows,
        new_rows,
        normalized_cutoff,
        skipped_rows,
    )
    
    return new_data

