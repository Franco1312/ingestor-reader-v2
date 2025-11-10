"""Check if source file changed step."""
from typing import Optional

from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.use_cases.steps.fetch_resource import compute_file_hash
from ingestor_reader.infra.common import get_logger

logger = get_logger(__name__)


def check_source_changed(
    catalog: S3Catalog,
    dataset_id: str,
    source_content: bytes,
) -> tuple[bool, Optional[str]]:
    """
    Check if source file changed compared to last processed version.
    
    Args:
        catalog: S3 catalog instance
        dataset_id: Dataset ID
        source_content: Source file content bytes
        
    Returns:
        Tuple of (has_changed: bool, last_hash: Optional[str])
        - has_changed: True if file changed or first run, False if unchanged
        - last_hash: Hash of last processed file (None if first run)
    """

    current_hash = compute_file_hash(source_content)
    

    current_manifest = catalog.read_current_manifest(dataset_id)
    if current_manifest is None:
        logger.info("First run: no previous manifest found")
        return True, None
    

    last_version = current_manifest.get("current_version")
    if not last_version:
        logger.info("No previous version found")
        return True, None
    

    manifest = catalog.read_event_manifest(dataset_id, last_version)
    if manifest is None:
        logger.info("Last version manifest not found")
        return True, None
    
    try:
        

        source_files = manifest.get("source", {}).get("files", [])
        if not source_files:
            logger.info("No source files in last manifest")
            return True, None
        
        last_hash = source_files[0].get("sha256")
        if not last_hash:
            logger.info("No hash in last manifest")
            return True, None
        

        has_changed = current_hash != last_hash
        if has_changed:
            logger.info("Source changed: %s -> %s", last_hash[:8], current_hash[:8])
        else:
            logger.info("Source unchanged: %s", current_hash[:8])
        
        return has_changed, last_hash
        
    except (KeyError, ValueError) as e:
        logger.warning("Could not parse last manifest: %s. Processing anyway.", e)
        return True, None

