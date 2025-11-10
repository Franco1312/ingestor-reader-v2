"""S3 stores module."""
from ingestor_reader.infra.s3_stores.base import S3BaseStore
from ingestor_reader.infra.s3_stores.manifest_store import S3ManifestStore
from ingestor_reader.infra.s3_stores.index_store import S3IndexStore
from ingestor_reader.infra.s3_stores.event_store import S3EventStore
from ingestor_reader.infra.s3_stores.projection_store import S3ProjectionStore

__all__ = [
    "S3BaseStore",
    "S3ManifestStore",
    "S3IndexStore",
    "S3EventStore",
    "S3ProjectionStore",
]

