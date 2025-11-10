"""Pipeline orchestrator."""
import time
from typing import Optional
import pandas as pd

from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.domain.entities.app_config import AppConfig
from ingestor_reader.domain.entities.run import Run
from ingestor_reader.domain.entities.manifest import SourceFile
from ingestor_reader.domain.services.pipeline_service import generate_run_id, generate_version_ts
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.event_bus.sns_publisher import SNSPublisher
from ingestor_reader.infra.locks import DynamoDBLock
from ingestor_reader.infra.common import get_logger
from ingestor_reader.use_cases.steps.fetch_resource import fetch_resource, compute_file_hash
from ingestor_reader.use_cases.steps.check_source_changed import check_source_changed
from ingestor_reader.use_cases.steps.parse_file import parse_file
from ingestor_reader.use_cases.steps.filter_new_data import filter_new_data
from ingestor_reader.use_cases.steps.normalize_rows import normalize_rows
from ingestor_reader.use_cases.steps.compute_delta import compute_delta_step
from ingestor_reader.use_cases.steps.enrich_metadata import enrich_metadata
from ingestor_reader.use_cases.steps.write_events import write_events
from ingestor_reader.use_cases.steps.publish_version import publish_version
from ingestor_reader.use_cases.steps.consolidate_projection import consolidate_projection_step
from ingestor_reader.use_cases.steps.notify_consumers import notify_consumers

logger = get_logger(__name__)


def _get_lock_manager(app_config: AppConfig) -> DynamoDBLock | None:
    """
    Get lock manager if configured.
    
    Args:
        app_config: Application configuration
        
    Returns:
        Lock manager or None if not configured
    """
    if app_config.dynamodb_lock_table:
        return DynamoDBLock(
            table_name=app_config.dynamodb_lock_table,
            region=app_config.aws_region,
        )
    return None


def _initialize_infrastructure(app_config: AppConfig) -> tuple[S3Catalog, SNSPublisher, DynamoDBLock | None]:
    """Initialize infrastructure adapters."""
    s3_storage = S3Storage(bucket=app_config.s3_bucket, region=app_config.aws_region)
    catalog = S3Catalog(s3_storage)
    publisher = SNSPublisher(region=app_config.aws_region)
    lock_manager = _get_lock_manager(app_config)
    
    return catalog, publisher, lock_manager


def run_pipeline(
    config: DatasetConfig,
    app_config: AppConfig,
    run_id: Optional[str] = None,
    full_reload: bool = False,
) -> Run:
    """
    Run ETL pipeline.
    
    Args:
        config: Dataset configuration
        app_config: Application configuration
        run_id: Optional run ID (generated if None)
        full_reload: If True, process even if source unchanged (default: False)
        
    Returns:
        Run metadata
    """

    if run_id is None:
        run_id = generate_run_id()
    version_ts = generate_version_ts()
    run = Run(dataset_id=config.dataset_id, run_id=run_id, version_ts=version_ts)
    
    logger.info("Starting pipeline: %s run=%s", config.dataset_id, run_id)
    pipeline_start = time.time()
    

    catalog, publisher, lock_manager = _initialize_infrastructure(app_config)
    

    lock_key = f"pipeline:{config.dataset_id}"
    lock_acquired = False
    if lock_manager:
        lock_acquired = lock_manager.acquire(lock_key, run_id)
        if not lock_acquired:
            logger.warning("Pipeline already running for %s, skipping execution", config.dataset_id)
            return run
    
    try:
        # Verify pointer-index consistency and rebuild if needed
        if not catalog.verify_pointer_index_consistency(config.dataset_id):
            logger.warning("Pointer-index inconsistency detected, rebuilding index...")
            catalog.rebuild_index_from_pointer(config.dataset_id)
            logger.info("Index rebuilt successfully")

        content, file_hash, file_size = step_fetch_resource(
            config, app_config
        )
        

        if not step_check_source_changed(catalog, config.dataset_id, content, full_reload):
            logger.info("Pipeline completed: no changes detected")
            return run
        

        parsed_df = step_parse_file(content, config)
        

        new_df = step_filter_new_data(catalog, config.dataset_id, parsed_df)
        if len(new_df) == 0:
            logger.info("Pipeline completed: no new data")
            return run
        

        normalized_df = step_normalize_rows(new_df, config)
        

        delta_df, current_index_df = step_compute_delta(catalog, config, normalized_df)
        

        enriched_delta_df = step_enrich_metadata(delta_df, config, version_ts)
        

        event_keys, rows_added = step_write_events(
            catalog, config, version_ts, enriched_delta_df
        )
        

        source_file = (file_hash, file_size)
        published = step_publish_version(
            catalog, config, version_ts, source_file, event_keys, rows_added,
            current_index_df, delta_df
        )
        

        if published:
            consolidate_start = time.time()
            step_consolidate_projection(catalog, config, enriched_delta_df)
            consolidate_time = time.time() - consolidate_start
            logger.info("Consolidation completed in %.2f seconds", consolidate_time)
        

        if published:
            step_notify_consumers(catalog, publisher, config, app_config, version_ts)
    
        pipeline_time = time.time() - pipeline_start
        logger.info("Pipeline completed: %d normalized, %d added (total time: %.2f seconds)", 
                    len(normalized_df), rows_added, pipeline_time)
        
        return run
    finally:

        if lock_manager and lock_acquired:
            lock_manager.release(lock_key, run_id)


def step_fetch_resource(
    config: DatasetConfig,
    app_config: AppConfig,
) -> tuple[bytes, str, int]:
    """Step: Fetch source file."""
    if not config.source.url:
        raise ValueError("URL required in source config")
    
    content = fetch_resource(config.source.url, verify_ssl=app_config.verify_ssl)
    file_hash = compute_file_hash(content)
    file_size = len(content)
    
    logger.info("Fetched %d bytes, hash=%s", file_size, file_hash[:8])
    
    return content, file_hash, file_size


def step_check_source_changed(
    catalog: S3Catalog,
    dataset_id: str,
    content: bytes,
    full_reload: bool,
) -> bool:
    """Step: Check if source file changed."""
    if full_reload:
        logger.info("Full reload requested, processing regardless of source changes")
        return True
    
    has_changed, _ = check_source_changed(catalog, dataset_id, content)
    if not has_changed:
        logger.info("Source unchanged, skipping processing")
    return has_changed


def step_parse_file(
    content: bytes,
    config: DatasetConfig,
) -> pd.DataFrame:
    """Step: Parse file."""
    df = parse_file(content, config)
    logger.info("Parsed %d rows", len(df))
    return df


def step_filter_new_data(
    catalog: S3Catalog,
    dataset_id: str,
    parsed_df: pd.DataFrame,
) -> pd.DataFrame:
    """Step: Filter parsed data to only include new rows (by date)."""
    new_df = filter_new_data(catalog, dataset_id, parsed_df)
    if len(new_df) == 0:
        logger.info("No new data to process")
    else:
        logger.info("Filtered to %d new rows", len(new_df))
    return new_df


def step_normalize_rows(
    df: pd.DataFrame,
    config: DatasetConfig,
) -> pd.DataFrame:
    """Step: Normalize rows."""
    normalized_df = normalize_rows(df, config)
    logger.info("Normalized %d rows", len(normalized_df))
    return normalized_df


def step_compute_delta(
    catalog: S3Catalog,
    config: DatasetConfig,
    normalized_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Step: Compute which rows are new (delta)."""
    current_index_df = catalog.read_index(config.dataset_id)
    delta_df = compute_delta_step(
        normalized_df, current_index_df, config.normalize.primary_keys
    )
    logger.info("Delta: %d new rows", len(delta_df))
    return delta_df, current_index_df


def step_enrich_metadata(
    delta_df: pd.DataFrame,
    config: DatasetConfig,
    version_ts: str,
) -> pd.DataFrame:
    """Step: Enrich delta with metadata."""
    if len(delta_df) == 0:
        return delta_df.copy()
    

    delta_without_hash = delta_df.drop(columns=["key_hash"], errors="ignore").copy()
    enriched_delta_df = enrich_metadata(delta_without_hash, config, version_ts)
    logger.info("Enriched %d rows with metadata", len(enriched_delta_df))
    return enriched_delta_df


def step_write_events(
    catalog: S3Catalog,
    config: DatasetConfig,
    version_ts: str,
    enriched_delta_df: pd.DataFrame,
) -> tuple[list[str], int]:
    """Step: Write events to S3."""
    event_keys, rows_added = write_events(
        catalog, config, version_ts, enriched_delta_df
    )
    logger.info("Wrote %d event files, %d rows", len(event_keys), rows_added)
    return event_keys, rows_added


def step_consolidate_projection(
    catalog: S3Catalog,
    config: DatasetConfig,
    enriched_delta_df: pd.DataFrame,
) -> None:
    """Step: Consolidate events into projection windows."""
    consolidate_projection_step(catalog, config, enriched_delta_df)
    logger.info("Consolidated projections")


def step_publish_version(
    catalog: S3Catalog,
    config: DatasetConfig,
    version_ts: str,
    source_file,
    output_keys: list[str],
    rows_added: int,
    current_index_df: pd.DataFrame | None,
    delta_df: pd.DataFrame,
) -> bool:
    """Step: Publish version atomically."""
    from ingestor_reader.domain.entities.manifest import SourceFile as SourceFileEntity
    
    current_etag = catalog.get_current_manifest_etag(config.dataset_id)
    source_file_entity = SourceFileEntity(
        path=None, sha256=source_file[0], size=source_file[1]
    )
    
    published = publish_version(
        catalog,
        config.dataset_id,
        version_ts,
        source_file_entity,
        output_keys,
        rows_added,
        config.normalize.primary_keys,
        current_index_df,
        delta_df,
        current_etag,
    )
    
    if published:
        logger.info("Published version %s", version_ts)
    else:
        logger.info("Publish skipped (0 rows or CAS failed)")
    
    return published


def step_notify_consumers(
    catalog: S3Catalog,
    publisher: SNSPublisher,
    config: DatasetConfig,
    app_config: AppConfig,
    version_ts: str,
) -> None:
    """Step: Notify consumers of new event."""
    topic_arn = (config.notify.sns_topic_arn if config.notify else None) or app_config.sns_topic_arn
    manifest_pointer = catalog.get_event_manifest_pointer(config.dataset_id, version_ts)
    notify_consumers(publisher, topic_arn, config.dataset_id, manifest_pointer)
    logger.info("Notified consumers")



