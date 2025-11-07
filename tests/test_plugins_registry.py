"""Tests for plugin registry."""
import pytest
import pandas as pd

from ingestor_reader.infra.plugins.registry import (
    get_parser,
    get_normalizer,
    register_parser,
    register_normalizer,
)
from ingestor_reader.domain.plugins.base import ParserPlugin, NormalizerPlugin
from ingestor_reader.domain.entities.dataset_config import (
    DatasetConfig,
    SourceConfig,
    ParseConfig,
    NormalizeConfig,
)


class TestParserPlugin(ParserPlugin):
    """Test parser plugin."""
    id = "test_parser"
    
    def parse(self, config, raw_bytes: bytes) -> pd.DataFrame:
        return pd.DataFrame({"obs_time": ["2024-01-01"], "value": [1.0], "internal_series_code": ["TEST"]})


class TestNormalizerPlugin(NormalizerPlugin):
    """Test normalizer plugin."""
    id = "test_normalizer"
    
    def normalize(self, config, df: pd.DataFrame) -> pd.DataFrame:
        return df.copy()


def test_explicit_parser_lookup():
    """Test explicit plugin lookup works."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    config = DatasetConfig(
        dataset_id="test",
        frequency="D",
        lag_days=1,
        source=SourceConfig(kind="http", url="test", format="xlsx"),
        parse=ParseConfig(plugin="bcra_infomondia"),
        normalize=NormalizeConfig(plugin="bcra_infomondia", primary_keys=["obs_time", "internal_series_code"]),
    )
    
    parser = get_parser("bcra_infomondia", config)
    assert parser.id == "bcra_infomondia"


def test_parser_requires_plugin_id():
    """Test that parser requires plugin ID (no fallback)."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    config = DatasetConfig(
        dataset_id="test",
        frequency="D",
        lag_days=1,
        source=SourceConfig(kind="http", url="test", format="xlsx"),
        parse=ParseConfig(),
        normalize=NormalizeConfig(primary_keys=["key"]),
    )
    
    with pytest.raises(ValueError, match="Plugin ID is required"):
        get_parser(None, config)


def test_custom_plugin_registration():
    """Test custom plugin can be registered and retrieved."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    # Register custom plugin
    test_parser = TestParserPlugin()
    register_parser(test_parser)
    
    config = DatasetConfig(
        dataset_id="test",
        frequency="D",
        lag_days=1,
        source=SourceConfig(kind="http", url="test", format="xlsx"),
        parse=ParseConfig(plugin="test_parser"),
        normalize=NormalizeConfig(plugin="test_normalizer", primary_keys=["key"]),
    )
    
    parser = get_parser("test_parser", config)
    assert parser.id == "test_parser"
    
    # Test it works
    result = parser.parse(config, b"test")
    assert isinstance(result, pd.DataFrame)


def test_normalizer_requires_plugin_id():
    """Test that normalizer requires plugin ID (no fallback)."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    with pytest.raises(ValueError, match="Plugin ID is required"):
        get_normalizer(None)


def test_normalizer_lookup():
    """Test normalizer lookup works."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    normalizer = get_normalizer("bcra_infomondia")
    assert normalizer.id == "bcra_infomondia"


def test_custom_normalizer_registration():
    """Test custom normalizer can be registered."""
    # Import to register plugins
    import ingestor_reader.infra.plugins  # noqa: F401
    
    test_normalizer = TestNormalizerPlugin()
    register_normalizer(test_normalizer)
    
    normalizer = get_normalizer("test_normalizer")
    assert normalizer.id == "test_normalizer"

