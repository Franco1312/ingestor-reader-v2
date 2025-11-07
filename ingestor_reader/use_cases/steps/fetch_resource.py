"""Fetch resource step."""
import hashlib
import os
import requests
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _get_cert_path() -> str | None:
    """
    Get path to custom certificate bundle if available.
    
    Returns:
        Path to certificate bundle or None if not found
    """
    # Check for custom certificate bundle in Lambda task root
    cert_path = Path("/var/task/certs/cacert.pem")
    if cert_path.exists():
        return str(cert_path)
    
    # Check for environment variable
    cert_path_env = os.getenv("SSL_CERT_FILE")
    if cert_path_env and Path(cert_path_env).exists():
        return cert_path_env
    
    return None


def fetch_resource(url: str, verify_ssl: bool = True) -> bytes:
    """
    Fetch resource from HTTP URL.
    
    Args:
        url: HTTP URL
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        Content bytes
    """
    logger.info("Fetching from HTTP: %s (verify_ssl=%s)", url, verify_ssl)
    
    # Determine certificate verification
    if verify_ssl:
        cert_path = _get_cert_path()
        if cert_path:
            logger.info("Using custom certificate bundle: %s", cert_path)
            verify = cert_path
        else:
            verify = True  # Use default system certificates
    else:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        verify = False
    
    response = requests.get(url, timeout=300, verify=verify)
    response.raise_for_status()
    return response.content


def compute_file_hash(content: bytes) -> str:
    """Compute SHA256 hash of file content."""
    return hashlib.sha256(content).hexdigest()

