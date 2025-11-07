"""Dataset configuration entity."""
from typing import Literal
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Source configuration."""
    kind: Literal["http", "local"]
    url: str | None = None
    format: Literal["csv", "xlsx"]
    sheet: str | None = None
    header_row: int | None = None


class ParseConfig(BaseModel):
    """Parse configuration."""
    plugin: str | None = None


class NormalizeConfig(BaseModel):
    """Normalization configuration."""
    plugin: str | None = None
    primary_keys: list[str]
    timezone: str | None = None


class OutputConfig(BaseModel):
    """Output configuration."""
    pass


class NotifyConfig(BaseModel):
    """Notification configuration."""
    sns_topic_arn: str | None = None


class DatasetConfig(BaseModel):
    """Dataset configuration."""
    dataset_id: str
    frequency: str
    lag_days: int
    source: SourceConfig
    parse: ParseConfig
    normalize: NormalizeConfig
    output: OutputConfig = Field(default_factory=OutputConfig)
    notify: NotifyConfig | None = None
    parse_config: dict | None = None
    provider: str | None = None
    unit: str | None = None
    plugin: str | None = None

