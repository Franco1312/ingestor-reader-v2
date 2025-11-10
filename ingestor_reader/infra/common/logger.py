"""Centralized logging configuration."""
import logging
import sys
from typing import Optional


_logging_configured = False


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    datefmt: Optional[str] = None,
    force: bool = False,
) -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Logging level
        format_string: Custom format string. If None, uses default.
        datefmt: Date format string. If None, uses default.
        force: If True, reconfigure even if already configured.
    """
    global _logging_configured
    
    if _logging_configured and not force:
        return
    
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    if datefmt is None:
        datefmt = "%Y-%m-%d %H:%M:%S"
    
    logging.basicConfig(
        level=level,
        format=format_string,
        datefmt=datefmt,
        stream=sys.stdout,
        force=force,
    )
    
    _logging_configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    if not _logging_configured:
        setup_logging()
    
    return logging.getLogger(name)

