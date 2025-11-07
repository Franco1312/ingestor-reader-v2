"""BCRA Infomondia plugin."""
from ingestor_reader.infra.plugins.registry import register_parser, register_normalizer
from ingestor_reader.infra.plugins.bcra_infomondia.parser import ParserBCRAInfomondia
from ingestor_reader.infra.plugins.bcra_infomondia.normalizer import NormalizerBCRAInfomondia

register_parser(ParserBCRAInfomondia())
register_normalizer(NormalizerBCRAInfomondia())

