"""Consolidate projection step."""
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.use_cases.steps.consolidation import ConsolidationOrchestrator


def consolidate_projection_step(
    catalog: S3Catalog,
    config: DatasetConfig,
    enriched_delta_df,
) -> None:
    """
    Consolidate events into projection windows.
    
    Args:
        catalog: S3 catalog instance
        config: Dataset configuration
        enriched_delta_df: Enriched delta DataFrame
    """
    orchestrator = ConsolidationOrchestrator(catalog)
    orchestrator.consolidate_projection_step(config, enriched_delta_df)
