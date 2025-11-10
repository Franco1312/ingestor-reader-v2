"""Consolidation manifest operations."""
from typing import Optional
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


class ConsolidationManifest:
    """Manages consolidation manifests for idempotency and restart resilience."""
    
    def __init__(self, catalog: S3Catalog):
        """Initialize consolidation manifest manager."""
        self.catalog = catalog
    
    def is_already_consolidated(self, dataset_id: str, year: int, month: int) -> bool:
        """
        Check if month is already consolidated.
        
        Args:
            dataset_id: Dataset ID
            year: Year
            month: Month (1-12)
            
        Returns:
            True if already consolidated, False otherwise
        """
        manifest = self.catalog.read_consolidation_manifest(dataset_id, year, month)
        return manifest is not None and manifest.get("status") == "completed"
    
    def mark_in_progress(self, dataset_id: str, year: int, month: int) -> None:
        """Mark consolidation as in progress."""
        self.catalog.write_consolidation_manifest(dataset_id, year, month, status="in_progress")
        logger.debug("Marked consolidation as in_progress for %d-%02d", year, month)
    
    def mark_completed(self, dataset_id: str, year: int, month: int) -> None:
        """Mark consolidation as completed."""
        self.catalog.write_consolidation_manifest(dataset_id, year, month, status="completed")
        logger.debug("Marked consolidation as completed for %d-%02d", year, month)

