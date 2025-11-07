"""Fetch resource step."""
import hashlib
import requests
import logging

logger = logging.getLogger(__name__)


def fetch_resource(url: str, verify_ssl: bool = True) -> bytes:
    """
    Fetch resource from HTTP URL.
    
    Args:
        url: HTTP URL
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        Content bytes
    """
    logger.info(f"Fetching from HTTP: {url}")
    response = requests.get(url, timeout=300, verify=verify_ssl)
    response.raise_for_status()
    return response.content


def compute_file_hash(content: bytes) -> str:
    """Compute SHA256 hash of file content."""
    return hashlib.sha256(content).hexdigest()

