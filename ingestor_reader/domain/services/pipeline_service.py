"""Pipeline service utilities."""
from ingestor_reader.infra.common.clock import get_clock


def generate_run_id() -> str:
    """Generate a unique run ID."""
    return get_clock().generate_uuid()


def generate_version_ts() -> str:
    """Generate version timestamp in ISO format."""
    return get_clock().generate_version_ts()

