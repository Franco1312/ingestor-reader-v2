"""Hash utilities."""
import hashlib


def compute_file_hash(content: bytes) -> str:
    """
    Compute SHA256 hash of file content.
    
    Args:
        content: File content bytes
        
    Returns:
        SHA256 hash as hex string
    """
    return hashlib.sha256(content).hexdigest()


def compute_string_hash(text: str) -> str:
    """
    Compute SHA256 hash of string.
    
    Args:
        text: String to hash
        
    Returns:
        SHA256 hash as hex string
    """
    return hashlib.sha256(text.encode()).hexdigest()

