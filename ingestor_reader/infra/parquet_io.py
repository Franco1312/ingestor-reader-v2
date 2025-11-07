"""Parquet I/O operations."""
import io
import pandas as pd


class ParquetIO:
    """Parquet I/O adapter."""
    
    def read_from_bytes(self, data: bytes) -> pd.DataFrame:
        """Read parquet from bytes."""
        buffer = io.BytesIO(data)
        return pd.read_parquet(buffer)
    
    def write_to_bytes(self, df: pd.DataFrame) -> bytes:
        """Write parquet to bytes."""
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        return buffer.getvalue()
    
    def read_from_path(self, path: str) -> pd.DataFrame:
        """Read parquet from file path or S3 URI."""
        return pd.read_parquet(path, engine="pyarrow")
    
    def write_to_path(self, df: pd.DataFrame, path: str) -> None:
        """Write parquet to file path or S3 URI."""
        df.to_parquet(path, index=False, engine="pyarrow")

