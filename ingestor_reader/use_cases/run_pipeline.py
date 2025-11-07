"""Pipeline orchestrator."""
import logging
from typing import Optional
import pandas as pd

from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.domain.entities.app_config import AppConfig
from ingestor_reader.domain.entities.run import Run
from ingestor_reader.domain.services.pipeline_service import generate_run_id, generate_version_ts
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.locks.dynamodb_lock import DynamoDBLock
from ingestor_reader.infra.event_bus.sns_publisher import SNSPublisher
from ingestor_reader.use_cases.steps.fetch_resource import fetch_resource, compute_file_hash
from ingestor_reader.use_cases.steps.check_source_changed import check_source_changed
from ingestor_reader.use_cases.steps.parse_file import parse_file
from ingestor_reader.use_cases.steps.filter_new_data import filter_new_data
from ingestor_reader.use_cases.steps.normalize_rows import normalize_rows
from ingestor_reader.use_cases.steps.compute_delta import compute_delta_step
from ingestor_reader.use_cases.steps.enrich_metadata import enrich_metadata
from ingestor_reader.use_cases.steps.write_outputs import write_outputs
from ingestor_reader.use_cases.steps.publish_version import publish_version
from ingestor_reader.use_cases.steps.notify_consumers import notify_consumers

logger = logging.getLogger(__name__)


def _initialize_infrastructure(app_config: AppConfig) -> tuple[S3Catalog, DynamoDBLock | None, SNSPublisher]:
    """Initialize infrastructure adapters."""
    s3_storage = S3Storage(bucket=app_config.s3_bucket, region=app_config.aws_region)
    catalog = S3Catalog(s3_storage)
    lock = DynamoDBLock(app_config.dynamodb_table, region=app_config.aws_region) if app_config.dynamodb_table else None
    publisher = SNSPublisher(region=app_config.aws_region)
    return catalog, lock, publisher


def _acquire_lock(lock: DynamoDBLock | None, dataset_id: str, run_id: str) -> None:
    """Acquire lock for dataset."""
    if lock:
        if not lock.acquire(dataset_id, run_id):
            logger.warning("Failed to acquire lock for %s", dataset_id)
            raise RuntimeError("Could not acquire lock")


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
    # Prepare run metadata
    if run_id is None:
        run_id = generate_run_id()
    version_ts = generate_version_ts()
    run = Run(dataset_id=config.dataset_id, run_id=run_id, version_ts=version_ts)
    
    logger.info("Starting pipeline: %s run=%s", config.dataset_id, run_id)
    
    # Initialize infrastructure
    catalog, lock, publisher = _initialize_infrastructure(app_config)
    
    try:
        # Acquire lock to prevent concurrent runs
        _acquire_lock(lock, config.dataset_id, run_id)
        
        # Step 1: Fetch resource
        content, raw_key, file_hash, file_size = step_fetch_resource(
            catalog, config, app_config, run_id
        )
        
        # Step 2: Check source changed
        if not step_check_source_changed(catalog, config.dataset_id, content, full_reload):
            logger.info("Pipeline completed: no changes detected")
            return run
        
        # Step 3: Parse file
        parsed_df = step_parse_file(content, config)
        
        # Step 4: Filter new data
        new_df = step_filter_new_data(catalog, config.dataset_id, parsed_df)
        if len(new_df) == 0:
            logger.info("Pipeline completed: no new data")
            return run
        
        # Step 5: Normalize rows
        normalized_df = step_normalize_rows(new_df, config)
        
        # Step 6: Compute delta
        delta_df, current_index_df = step_compute_delta(catalog, config, normalized_df)
        
        # Step 7: Enrich metadata
        enriched_delta_df = step_enrich_metadata(delta_df, config, version_ts)
        
        # Step 8: Write outputs
        output_keys, rows_added = step_write_outputs(
            catalog, config, run_id, version_ts, normalized_df, enriched_delta_df, delta_df
        )
        
        # Step 9: Publish version
        source_file = (raw_key, file_hash, file_size)
        published = step_publish_version(
            catalog, config, version_ts, source_file, output_keys, rows_added,
            current_index_df, delta_df
        )
        
        # Step 10: Notify consumers
        if published:
            step_notify_consumers(publisher, config, app_config, version_ts, rows_added)
        
        logger.info("Pipeline completed: %d normalized, %d added", len(normalized_df), rows_added)
        
    finally:
        # Always release lock
        if lock:
            try:
                lock.release(config.dataset_id, run_id)
            except (RuntimeError, ValueError, KeyError) as e:
                logger.warning("Failed to release lock: %s", e)
    
    return run


def step_fetch_resource(
    catalog: S3Catalog,
    config: DatasetConfig,
    app_config: AppConfig,
    run_id: str,
) -> tuple[bytes, str, str, int]:
    """Step: Fetch source file and store it in S3."""
    if not config.source.url:
        raise ValueError("URL required in source config")
    
    content = fetch_resource(config.source.url, verify_ssl=app_config.verify_ssl)
    file_hash = compute_file_hash(content)
    file_size = len(content)
    filename = config.source.url.split("/")[-1] or "resource"
    
    raw_key = catalog.write_run_raw(config.dataset_id, run_id, filename, content)
    logger.info("Fetched %d bytes, hash=%s", file_size, file_hash[:8])
    
    return content, raw_key, file_hash, file_size


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
    
    # Remove key_hash before enriching (it's only for internal calculation)
    delta_without_hash = delta_df.drop(columns=["key_hash"], errors="ignore").copy()
    enriched_delta_df = enrich_metadata(delta_without_hash, config, version_ts)
    logger.info("Enriched %d rows with metadata", len(enriched_delta_df))
    return enriched_delta_df


def step_write_outputs(
    catalog: S3Catalog,
    config: DatasetConfig,
    run_id: str,
    version_ts: str,
    normalized_df: pd.DataFrame,
    enriched_delta_df: pd.DataFrame,
    delta_df: pd.DataFrame,
) -> tuple[list[str], int]:
    """Step: Write outputs to S3."""
    output_keys, rows_added = write_outputs(
        catalog, config, run_id, version_ts, normalized_df, enriched_delta_df, delta_df
    )
    logger.info("Wrote %d files, %d rows", len(output_keys), rows_added)
    return output_keys, rows_added


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
        path=source_file[0], sha256=source_file[1], size=source_file[2]
    )
    
    published = publish_version(
        catalog,
        config.dataset_id,
        version_ts,
        source_file_entity,
        output_keys,
        rows_added,
        config.normalize.primary_keys,
        config.lag_days,
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
    publisher: SNSPublisher,
    config: DatasetConfig,
    app_config: AppConfig,
    version_ts: str,
    rows_added: int,
) -> None:
    """Step: Notify consumers of new version."""
    topic_arn = (config.notify.sns_topic_arn if config.notify else None) or app_config.sns_topic_arn
    notify_consumers(publisher, topic_arn, config.dataset_id, version_ts, rows_added)
    logger.info("Notified consumers")



