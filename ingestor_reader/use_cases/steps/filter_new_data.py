"""Filter new data by date step."""
from typing import Optional
import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.common import get_logger, find_date_column

logger = get_logger(__name__)


def _get_max_date_from_file(catalog: S3Catalog, file_key: str) -> Optional[pd.Timestamp]:
    """Get maximum date from a single output file."""
    try:
        parquet_body = catalog.s3.get_object(file_key)
        df = catalog.parquet_io.read_from_bytes(parquet_body)
        
        date_column = find_date_column(df)
        if not date_column:
            return None
        
        df[date_column] = pd.to_datetime(df[date_column])
        return df[date_column].max()
    except (KeyError, ValueError, AttributeError) as e:
        logger.warning("Could not read output file %s: %s", file_key, e)
        return None


def _get_last_event_manifest(catalog: S3Catalog, dataset_id: str) -> Optional[dict]:
    """Get last event manifest from catalog."""
    current_manifest = catalog.read_current_manifest(dataset_id)
    if current_manifest is None:
        return None
    
    last_version = current_manifest.get("current_version")
    if not last_version:
        return None
    
    return catalog.read_event_manifest(dataset_id, last_version)


def _get_latest_date_from_events(catalog: S3Catalog, dataset_id: str) -> Optional[pd.Timestamp]:
    """Get latest date from published events."""
    manifest = _get_last_event_manifest(catalog, dataset_id)
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
    

    if df_tz is None and cutoff_tz is not None:
        cutoff_date = cutoff_date.tz_localize(None)

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

    latest_date = _get_latest_date_from_events(catalog, dataset_id)
    
    if latest_date is None:
        logger.info("No previous data found, processing all rows")
        return parsed_df
    

    if date_column not in parsed_df.columns:
        logger.warning("Date column %s not found, processing all rows", date_column)
        return parsed_df
    

    normalized_df, normalized_cutoff = _normalize_timezones(parsed_df, date_column, latest_date)
    

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

