"""Manifest builder for version publishing."""
from ingestor_reader.domain.entities.manifest import Manifest, SourceFile, OutputsInfo, IndexInfo
from ingestor_reader.infra.common import Clock, S3PathBuilder


class ManifestBuilder:
    """Builds manifests for version publishing."""
    
    def __init__(self, clock: Clock | None = None, paths: S3PathBuilder | None = None):
        """
        Initialize manifest builder.
        
        Args:
            clock: Clock instance (defaults to system clock)
            paths: Path builder instance (defaults to S3PathBuilder)
        """
        from ingestor_reader.infra.common import get_clock
        
        self.clock = clock or get_clock()
        self.paths = paths or S3PathBuilder()
    
    def build_manifest(
        self,
        dataset_id: str,
        version_ts: str,
        source_file: SourceFile,
        output_keys: list[str],
        rows_added: int,
        total_rows: int,
        primary_keys: list[str],
    ) -> Manifest:
        """
        Build manifest for a version.
        
        Args:
            dataset_id: Dataset ID
            version_ts: Version timestamp
            source_file: Source file metadata
            output_keys: Output file keys
            rows_added: Number of rows added in this version
            total_rows: Total rows after this version
            primary_keys: Primary key columns
            
        Returns:
            Manifest object
        """
        created_at = self.clock.now_iso()
        
        # Clean source file dict (remove None path)
        source_file_dict = source_file.model_dump()
        if source_file_dict.get("path") is None:
            source_file_dict.pop("path", None)
        
        return Manifest(
            dataset_id=dataset_id,
            version=version_ts,
            created_at=created_at,
            source={
                "files": [source_file_dict]
            },
            outputs=OutputsInfo(
                data_prefix=self.paths.events_prefix(dataset_id, version_ts),
                files=output_keys,
                rows_total=total_rows,
                rows_added_this_version=rows_added,
            ),
            index=IndexInfo(
                path=self.paths.index_key(dataset_id),
                key_columns=primary_keys,
                hash_column="key_hash",
            ),
        )

