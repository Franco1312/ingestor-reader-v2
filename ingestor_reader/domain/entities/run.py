"""Run entity."""
from pydantic import BaseModel


class Run(BaseModel):
    """ETL run metadata."""
    dataset_id: str
    run_id: str
    version_ts: str

