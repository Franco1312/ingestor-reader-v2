"""Manifest entity."""
from datetime import datetime
from typing import Any
from pydantic import BaseModel


class SourceFile(BaseModel):
    """Source file metadata."""
    path: str
    sha256: str
    size: int


class OutputsInfo(BaseModel):
    """Output information."""
    data_prefix: str
    files: list[str]
    rows_total: int
    rows_added_this_version: int


class IndexInfo(BaseModel):
    """Index information."""
    path: str
    key_columns: list[str]
    hash_column: str


class Manifest(BaseModel):
    """Published version manifest."""
    dataset_id: str
    version: str
    created_at: str
    source: dict[str, Any]
    outputs: OutputsInfo
    index: IndexInfo

