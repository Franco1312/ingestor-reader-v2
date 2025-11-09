"""Consolidate projection step."""
import logging

import pandas as pd

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.domain.services.consolidation_service import consolidate_month_projections
from ingestor_reader.use_cases.steps.utils import find_date_column

logger = logging.getLogger(__name__)


def _extract_year_month(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Extract year and month columns from date column.
    
    Args:
        df: DataFrame with date column
        date_col: Name of date column
        
    Returns:
        DataFrame with year and month columns added, invalid dates filtered out
    """
    df_with_partitions = df.copy()
    df_with_partitions["year"] = pd.to_datetime(df_with_partitions[date_col], errors="coerce").dt.year
    df_with_partitions["month"] = pd.to_datetime(df_with_partitions[date_col], errors="coerce").dt.month
    return df_with_partitions.dropna(subset=["year", "month"])


def _get_affected_months(df: pd.DataFrame) -> list[tuple[int, int]]:
    """
    Get list of affected (year, month) tuples from DataFrame.
    
    Args:
        df: DataFrame with year and month columns
        
    Returns:
        List of (year, month) tuples
    """
    months = df[["year", "month"]].drop_duplicates()
    return [(int(row["year"]), int(row["month"])) for _, row in months.iterrows()]


def _is_already_consolidated(catalog: S3Catalog, dataset_id: str, year: int, month: int) -> bool:
    """
    Check if month is already consolidated.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        year: Year
        month: Month (1-12)
        
    Returns:
        True if already consolidated, False otherwise
    """
    manifest = catalog.read_consolidation_manifest(dataset_id, year, month)
    return manifest is not None and manifest.get("status") == "completed"


def _write_series_projections(
    catalog: S3Catalog,
    dataset_id: str,
    year: int,
    month: int,
    series_projections: dict[str, pd.DataFrame],
) -> None:
    """
    Write all series projections for a month using WAL pattern.
    
    Writes to temporary location first, then moves atomically to final location.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        year: Year
        month: Month (1-12)
        series_projections: Dict mapping series_code to consolidated DataFrame
        
    Raises:
        Exception: If any write or move operation fails
    """
    # Write all projections to temporary location
    for series_code, consolidated_df in series_projections.items():
        catalog.write_series_projection_temp(
            dataset_id, series_code, year, month, consolidated_df
        )
        logger.debug("Written temp projection for %s %d-%02d (%d rows)", 
                   series_code, year, month, len(consolidated_df))
    
    # Move all projections atomically from temp to final
    for series_code in series_projections.keys():
        catalog.move_series_projection_from_temp(dataset_id, series_code, year, month)
        logger.info("Moved projection for %s %d-%02d to final location", 
                   series_code, year, month)


def consolidate_projection_step(
    catalog: S3Catalog,
    config: DatasetConfig,
    enriched_delta_df: pd.DataFrame,
) -> None:
    """
    Consolidate events into projection windows.
    
    For each year/month affected by the delta, consolidates all events
    into a projection window.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        enriched_delta_df: Enriched delta DataFrame
    """
    if len(enriched_delta_df) == 0:
        return
    
    date_col = find_date_column(enriched_delta_df)
    if date_col is None:
        logger.warning("No date column found, skipping consolidation")
        return
    
    if "internal_series_code" not in enriched_delta_df.columns:
        logger.warning("No internal_series_code column found, skipping consolidation")
        return
    
    df_with_partitions = _extract_year_month(enriched_delta_df, date_col)
    affected_months = _get_affected_months(df_with_partitions)
    
    if not affected_months:
        logger.info("No valid months found, skipping consolidation")
        return
    
    logger.info("Consolidating projections for %d affected month(s)", len(affected_months))
    
    for year, month in affected_months:
        try:
            _consolidate_month(
                catalog, config, year, month, config.normalize.primary_keys
            )
        except Exception as e:
            logger.error("Failed to consolidate projections for %d-%02d: %s", 
                       year, month, e)


def _consolidate_month(
    catalog: S3Catalog,
    config: DatasetConfig,
    year: int,
    month: int,
    primary_keys: list[str],
) -> None:
    """
    Consolidate projections for a specific month with restart resilience.
    
    Uses manifest for idempotency and WAL pattern for atomic writes.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        year: Year
        month: Month (1-12)
        primary_keys: Primary key columns
    """
    dataset_id = config.dataset_id
    
    # Check if already consolidated (idempotency)
    if _is_already_consolidated(catalog, dataset_id, year, month):
        logger.info("Skipping %d-%02d (already consolidated)", year, month)
        return
    
    # Clean up any leftover temp files
    catalog.cleanup_temp_projections(dataset_id, year, month)
    
    # Mark as in progress
    catalog.write_consolidation_manifest(dataset_id, year, month, status="in_progress")
    
    try:
        # Consolidate all events for the month
        series_projections = consolidate_month_projections(
            catalog, dataset_id, year, month, primary_keys
        )
        
        if not series_projections:
            logger.warning("No series projections to write for %d-%02d", year, month)
            return
        
        # Write all projections using WAL pattern (temp -> final)
        _write_series_projections(catalog, dataset_id, year, month, series_projections)
        
        # Mark as completed
        catalog.write_consolidation_manifest(dataset_id, year, month, status="completed")
        logger.info("Completed consolidation for %d-%02d (%d series)", 
                   year, month, len(series_projections))
        
    except Exception as e:
        logger.error("Failed to consolidate %d-%02d: %s", year, month, e)
        catalog.cleanup_temp_projections(dataset_id, year, month)
        raise

