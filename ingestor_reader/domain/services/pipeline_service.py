"""Pipeline service utilities."""
from datetime import datetime, timezone
import uuid


def generate_run_id() -> str:
    """Generate a unique run ID."""
    return str(uuid.uuid4())


def generate_version_ts() -> str:
    """Generate version timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat().replace(":", "-").split(".")[0]


def format_version_ts(ts: str) -> str:
    """Format version timestamp for display."""
    return ts.replace("-", ":").replace("T", " ")


def compute_stats(df) -> dict[str, int]:
    """Compute basic statistics from DataFrame."""
    return {
        "rows": len(df),
        "columns": len(df.columns) if hasattr(df, "columns") else 0,
    }

