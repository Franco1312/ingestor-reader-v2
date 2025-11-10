"""Consolidation writer with WAL pattern."""
import pandas as pd
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


class ConsolidationWriter:
    """Writes series projections using WAL pattern for atomic operations."""
    
    def __init__(self, catalog: S3Catalog):
        """Initialize consolidation writer."""
        self.catalog = catalog
    
    def write_series_projections(
        self,
        dataset_id: str,
        year: int,
        month: int,
        series_projections: dict[str, pd.DataFrame],
    ) -> None:
        """
        Write all series projections for a month using WAL pattern.
        
        Writes to temporary location first, then moves atomically to final location.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
            series_projections: Dict mapping series_code to consolidated DataFrame
            
        Raises:
            Exception: If any write or move operation fails
        """
        # Write all projections to temporary location
        for series_code, consolidated_df in series_projections.items():
            self.catalog.write_series_projection_temp(
                dataset_id, series_code, year, month, consolidated_df
            )
            logger.debug("Written temp projection for %s %d-%02d (%d rows)", 
                       series_code, year, month, len(consolidated_df))
        
        # Move all projections atomically from temp to final
        for series_code in series_projections.keys():
            self.catalog.move_series_projection_from_temp(dataset_id, series_code, year, month)
            logger.info("Moved projection for %s %d-%02d to final location", 
                       series_code, year, month)
    
    def cleanup_temp(self, dataset_id: str, year: int, month: int) -> None:
        """Clean up temporary projections for a month."""
        self.catalog.cleanup_temp_projections(dataset_id, year, month)
        logger.debug("Cleaned up temp projections for %d-%02d", year, month)

