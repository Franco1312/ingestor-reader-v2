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


def _write_series_projections(
    catalog: S3Catalog,
    dataset_id: str,
    year: int,
    month: int,
    series_projections: dict[str, pd.DataFrame],
) -> None:
    """
    Write all series projections for a month.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        year: Year
        month: Month (1-12)
        series_projections: Dict mapping series_code to consolidated DataFrame
    """
    for series_code, consolidated_df in series_projections.items():
        try:
            catalog.write_series_projection(
                dataset_id, series_code, year, month, consolidated_df
            )
            logger.info("Written series projection for %s %d-%02d (%d rows)", 
                       series_code, year, month, len(consolidated_df))
        except Exception as e:
            logger.error("Failed to write series projection for %s %d-%02d: %s", 
                       series_code, year, month, e)


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
    
    logger.info("Consolidating projections for affected months")
    

    date_col = find_date_column(enriched_delta_df)
    if date_col is None:
        logger.warning("No date column found, skipping consolidation")
        return
    

    df_with_partitions = _extract_year_month(enriched_delta_df, date_col)
    

    if "internal_series_code" not in df_with_partitions.columns:
        logger.warning("No internal_series_code column found, skipping consolidation")
        return
    

    affected_months = df_with_partitions[["year", "month"]].drop_duplicates()
    
    logger.info("Processing %d affected month(s)", len(affected_months))
    
    for _, row in affected_months.iterrows():
        year = int(row["year"])
        month = int(row["month"])
        
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
    Consolidate projections for a specific month.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        year: Year
        month: Month (1-12)
        primary_keys: Primary key columns
    """

    series_projections = consolidate_month_projections(
        catalog,
        config.dataset_id,
        year,
        month,
        primary_keys,
    )
    

    _write_series_projections(
        catalog,
        config.dataset_id,
        year,
        month,
        series_projections,
    )

