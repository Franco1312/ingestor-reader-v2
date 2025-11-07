"""Plugin registry."""
from typing import Optional

from ingestor_reader.domain.plugins.base import ParserPlugin, NormalizerPlugin


# In-memory registry
PARSERS: dict[str, ParserPlugin] = {}
NORMALIZERS: dict[str, NormalizerPlugin] = {}


def register_parser(plugin: ParserPlugin) -> None:
    """Register a parser plugin."""
    PARSERS[plugin.id] = plugin


def register_normalizer(plugin: NormalizerPlugin) -> None:
    """Register a normalizer plugin."""
    NORMALIZERS[plugin.id] = plugin


def get_parser(plugin_id: Optional[str], config) -> ParserPlugin:
    """
    Get parser plugin.
    
    Args:
        plugin_id: Plugin ID from config
        config: Dataset configuration
        
    Returns:
        Parser plugin instance
        
    Raises:
        ValueError: If no parser found
    """
    if not plugin_id:
        raise ValueError("Plugin ID is required - no default parser available")
    
    if plugin_id not in PARSERS:
        raise ValueError(f"Parser plugin '{plugin_id}' not found")
    
    return PARSERS[plugin_id]


def get_normalizer(plugin_id: Optional[str]) -> NormalizerPlugin:
    """
    Get normalizer plugin.
    
    Args:
        plugin_id: Plugin ID from config
        
    Returns:
        Normalizer plugin instance
        
    Raises:
        ValueError: If no normalizer found
    """
    if not plugin_id:
        raise ValueError("Plugin ID is required - no default normalizer available")
    
    if plugin_id not in NORMALIZERS:
        raise ValueError(f"Normalizer plugin '{plugin_id}' not found")
    
    return NORMALIZERS[plugin_id]

