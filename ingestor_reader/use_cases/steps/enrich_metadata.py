"""Enrich metadata step."""
import pandas as pd

from ingestor_reader.domain.entities.dataset_config import DatasetConfig
from ingestor_reader.infra.common import get_logger, get_clock
from ingestor_reader.infra.common.series import resolve_series_code

logger = get_logger(__name__)


def _add_dataset_metadata(df: pd.DataFrame, config: DatasetConfig) -> None:
    """Add dataset_id and provider columns."""
    df["dataset_id"] = config.dataset_id
    df["provider"] = config.provider or ""


def _add_frequency_and_unit(df: pd.DataFrame, config: DatasetConfig) -> None:
    """Add frequency and unit, using DataFrame values if present, otherwise config."""
    if "frequency" not in df.columns:
        df["frequency"] = config.frequency
    if "unit" not in df.columns:
        df["unit"] = config.unit or ""


def _add_source_kind(df: pd.DataFrame, config: DatasetConfig) -> None:
    """Add source_kind: FILE if source has file format, otherwise based on source.kind."""
    if config.source.format:
        df["source_kind"] = "FILE"
    else:
        source_kind_map = {"http": "API", "local": "FILE"}
        df["source_kind"] = source_kind_map.get(config.source.kind, "FILE")


def _add_obs_date(df: pd.DataFrame) -> None:
    """Derive obs_date from obs_time (local date without time)."""
    if "obs_time" in df.columns:
        df["obs_date"] = pd.to_datetime(df["obs_time"]).dt.date
    else:
        df["obs_date"] = None


def _add_version_metadata(df: pd.DataFrame, version_ts: str) -> None:
    """Add version and vintage_date columns."""
    clock = get_clock()
    df["version"] = version_ts
    df["vintage_date"] = clock.now()


def _add_quality_flag(df: pd.DataFrame) -> None:
    """Add quality_flag column with default value 'OK'."""
    df["quality_flag"] = "OK"


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns to match expected schema order."""
    expected_order = [
        "dataset_id",
        "provider",
        "frequency",
        "unit",
        "source_kind",
        "obs_time",
        "obs_date",
        "value",
        "internal_series_code",
        "version",
        "vintage_date",
        "quality_flag",
    ]
    columns_to_include = [col for col in expected_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in expected_order]
    return df[columns_to_include + remaining_cols]


def enrich_metadata(
    df: pd.DataFrame,
    config: DatasetConfig,
    version_ts: str,
) -> pd.DataFrame:
    """
    Enrich DataFrame with metadata columns.
    
    Adds:
    - dataset_id: string
    - provider: string
    - frequency: string (from DataFrame if present, otherwise from config)
    - unit: string (from DataFrame if present, otherwise from config)
    - source_kind: string ("FILE" / "API")
    - obs_date: date (derived from obs_time)
    - version: string
    - vintage_date: datetime
    - quality_flag: string (default "OK")
    
    Args:
        df: DataFrame to enrich
        config: Dataset configuration
        version_ts: Version timestamp
        
    Returns:
        Enriched DataFrame with metadata columns
    """
    logger.info("Enriching %d rows with metadata", len(df))
    
    df = df.copy()
    
    # Resolve series_code (add if missing)
    df = resolve_series_code(df, config)
    
    _add_dataset_metadata(df, config)
    _add_frequency_and_unit(df, config)
    _add_source_kind(df, config)
    _add_obs_date(df)
    _add_version_metadata(df, version_ts)
    _add_quality_flag(df)
    
    result = _reorder_columns(df)
    
    logger.info("Enriched %d rows with metadata", len(result))
    return result

