"""Consolidation orchestrator."""
import pandas as pd
from typing import Optional

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.domain.services.consolidation_service import consolidate_month_projections
from ingestor_reader.infra.common import get_logger, find_date_column, add_year_month_partitions
from ingestor_reader.use_cases.steps.consolidation.manifest import ConsolidationManifest
from ingestor_reader.use_cases.steps.consolidation.writer import ConsolidationWriter

logger = get_logger(__name__)


class ConsolidationOrchestrator:
    """Orchestrates consolidation of events into projection windows."""
    
    def __init__(self, catalog: S3Catalog):
        """Initialize consolidation orchestrator."""
        self.catalog = catalog
        self.manifest = ConsolidationManifest(catalog)
        self.writer = ConsolidationWriter(catalog)
    
    def consolidate_projection_step(
        self,
        config: DatasetConfig,
        enriched_delta_df: pd.DataFrame,
    ) -> None:
        """
        Consolidate events into projection windows.
        
        For each year/month affected by the delta, consolidates all events
        into a projection window.
        
        Args:
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
        
        df_with_partitions = add_year_month_partitions(enriched_delta_df, date_col, drop_invalid=True)
        affected_months = self._get_affected_months(df_with_partitions)
        
        if not affected_months:
            logger.info("No valid months found, skipping consolidation")
            return
        
        logger.info("Consolidating projections for %d affected month(s)", len(affected_months))
        
        # Convert to set for fast lookup
        affected_months_set = set(affected_months)
        
        for year, month in affected_months:
            try:
                self._consolidate_month(
                    config, year, month, config.normalize.primary_keys,
                    affected_months=affected_months_set
                )
            except Exception as e:
                logger.error("Failed to consolidate projections for %d-%02d: %s", 
                           year, month, e)
    
    def _get_affected_months(self, df: pd.DataFrame) -> list[tuple[int, int]]:
        """
        Get list of affected (year, month) tuples from DataFrame.
        
        Args:
            df: DataFrame with year and month columns
            
        Returns:
            List of (year, month) tuples, sorted by year and month
        """
        months = df[["year", "month"]].drop_duplicates()
        result = [(int(row["year"]), int(row["month"])) for _, row in months.iterrows()]
        return sorted(result, key=lambda x: (x[0], x[1]))
    
    def _consolidate_month(
        self,
        config: DatasetConfig,
        year: int,
        month: int,
        primary_keys: list[str],
        affected_months: set[tuple[int, int]] | None = None,
    ) -> None:
        """
        Consolidate projections for a specific month with restart resilience.
        
        Uses manifest for idempotency and WAL pattern for atomic writes.
        Re-consolidates if month has new data (in affected_months).
        
        Args:
            config: Dataset configuration
            year: Year
            month: Month (1-12)
            primary_keys: Primary key columns
            affected_months: Set of (year, month) tuples with new data.
                            If month is in this set, always re-consolidate.
                            If None, only check manifest (idempotency for restarts).
        """
        dataset_id = config.dataset_id
        
        # If month has new data, always re-consolidate (ignore manifest)
        # Otherwise, check manifest for idempotency (restart resilience)
        has_new_data = affected_months is not None and (year, month) in affected_months
        
        if not has_new_data:
            # No new data: check manifest for idempotency (restart resilience)
            if self.manifest.is_already_consolidated(dataset_id, year, month):
                logger.info("Skipping %d-%02d (already consolidated, no new data)", year, month)
                return
        else:
            # Has new data: always re-consolidate (ignore manifest)
            logger.info("Re-consolidating %d-%02d (has new data)", year, month)
        
        # Clean up any leftover temp files
        self.writer.cleanup_temp(dataset_id, year, month)
        
        # Mark as in progress
        self.manifest.mark_in_progress(dataset_id, year, month)
        
        try:
            # Consolidate all events for the month
            series_projections = consolidate_month_projections(
                self.catalog, dataset_id, year, month, primary_keys
            )
            
            if not series_projections:
                logger.warning("No series projections to write for %d-%02d", year, month)
                return
            
            # Write all projections using WAL pattern (temp -> final)
            self.writer.write_series_projections(dataset_id, year, month, series_projections)
            
            # Mark as completed
            self.manifest.mark_completed(dataset_id, year, month)
            logger.info("Completed consolidation for %d-%02d (%d series)", 
                       year, month, len(series_projections))
            
        except Exception as e:
            logger.error("Failed to consolidate %d-%02d: %s", year, month, e)
            self.writer.cleanup_temp(dataset_id, year, month)
            raise

