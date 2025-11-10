"""Consolidation service for projections."""
import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


def _read_events_for_month(
    catalog: S3Catalog,
    event_keys: list[str],
) -> list[pd.DataFrame]:
    """
    Read all event DataFrames for a month.
    
    Args:
        catalog: S3 catalog instance
        event_keys: List of event keys to read
        
    Returns:
        List of DataFrames (one per event)
    """
    all_events = []
    for event_key in event_keys:
        try:
            body = catalog.s3.get_object(event_key)
            df = catalog.parquet_io.read_from_bytes(body)
            
            if "internal_series_code" not in df.columns:
                logger.warning("No internal_series_code column in event %s", event_key)
                continue
            
            all_events.append(df)
            logger.debug("Read event %s: %d rows", event_key, len(df))
        except Exception as e:
            logger.warning("Failed to read event %s: %s", event_key, e)
            continue
    
    return all_events


def _deduplicate_dataframe(
    df: pd.DataFrame,
    primary_keys: list[str],
) -> pd.DataFrame:
    """
    Remove duplicates from DataFrame, keeping the most recent version.
    
    Args:
        df: DataFrame to deduplicate
        primary_keys: Primary key columns for deduplication
        
    Returns:
        Deduplicated DataFrame
    """
    if "version" in df.columns:

        df = df.sort_values("version", ascending=False)
    

    return df.drop_duplicates(subset=primary_keys, keep="first")


def consolidate_month_projections(
    catalog: S3Catalog,
    dataset_id: str,
    year: int,
    month: int,
    primary_keys: list[str],
) -> dict[str, pd.DataFrame]:
    """
    Consolidate all events for a month and group by series.
    
    Reads all events for the month once, groups by internal_series_code,
    and returns a dict of series_code -> consolidated DataFrame.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        year: Year
        month: Month (1-12)
        primary_keys: Primary key columns for deduplication
        
    Returns:
        Dict mapping series_code to consolidated DataFrame
    """
    logger.info("Consolidating all series projections for %s year=%d month=%02d", 
                dataset_id, year, month)
    

    event_keys = catalog.list_events_for_month(dataset_id, year, month)
    
    if not event_keys:
        logger.info("No events found for %d-%02d, skipping consolidation", year, month)
        return {}
    
    logger.info("Found %d events for %d-%02d, reading once and grouping by series", 
                len(event_keys), year, month)
    

    all_events = _read_events_for_month(catalog, event_keys)
    
    if not all_events:
        logger.warning("No valid events found for %d-%02d", year, month)
        return {}
    

    all_data = pd.concat(all_events, ignore_index=True)
    logger.info("Read %d total rows from %d events", len(all_data), len(all_events))
    

    logger.info("Grouping by series (found %d unique series)", 
                all_data["internal_series_code"].nunique())
    
    series_projections = {}
    for series_code, series_data in all_data.groupby("internal_series_code"):

        consolidated = _deduplicate_dataframe(series_data, primary_keys)
        series_projections[series_code] = consolidated
        logger.debug("Consolidated %d rows for series %s", len(consolidated), series_code)
    
    logger.info("Consolidated %d series for %d-%02d", len(series_projections), year, month)
    
    return series_projections





