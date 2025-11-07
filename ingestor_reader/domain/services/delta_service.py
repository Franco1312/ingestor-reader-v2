"""Delta computation service."""
import hashlib
import pandas as pd


def compute_key_hash(row: pd.Series, key_columns: list[str]) -> str:
    """Compute SHA1 hash of primary key values."""
    key_values = [str(row[col]) for col in key_columns]
    key_string = "|".join(key_values)
    return hashlib.sha1(key_string.encode()).hexdigest()


def compute_delta(
    normalized_df: pd.DataFrame,
    index_df: pd.DataFrame | None,
    primary_keys: list[str],
    hash_column: str = "key_hash",
) -> pd.DataFrame:
    """
    Compute delta: rows in normalized_df not present in index_df.
    
    Args:
        normalized_df: New normalized data
        index_df: Existing index (None for first run)
        primary_keys: Primary key column names
        hash_column: Name of hash column in both DataFrames
        
    Returns:
        DataFrame with only new rows
    """
    # Compute key hashes for normalized data
    normalized_df = normalized_df.copy()
    normalized_df[hash_column] = normalized_df.apply(
        lambda row: compute_key_hash(row, primary_keys), axis=1
    )
    
    # First run: all rows are new
    if index_df is None or len(index_df) == 0:
        return normalized_df
    
    # Anti-join: keep rows not in index
    existing_hashes = set(index_df[hash_column].values)
    added_df = normalized_df[~normalized_df[hash_column].isin(existing_hashes)].copy()
    
    return added_df


def update_index(
    current_index_df: pd.DataFrame | None,
    added_df: pd.DataFrame,
    hash_column: str = "key_hash",
) -> pd.DataFrame:
    """
    Update index with new rows.
    
    Args:
        current_index_df: Current index (None for first run)
        added_df: New rows to add
        hash_column: Name of hash column
        
    Returns:
        Updated index DataFrame
    """
    if current_index_df is None or len(current_index_df) == 0:
        return added_df[[hash_column]].copy()
    
    # Append new hashes
    new_hashes = added_df[[hash_column]].copy()
    updated_index = pd.concat([current_index_df, new_hashes], ignore_index=True)
    return updated_index.drop_duplicates(subset=[hash_column], keep="first")

