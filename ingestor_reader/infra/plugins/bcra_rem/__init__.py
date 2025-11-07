"""BCRA REM plugin."""
from ingestor_reader.infra.plugins.registry import register_parser, register_normalizer
from ingestor_reader.infra.plugins.bcra_rem.parser import ParserBCRAREM
from ingestor_reader.infra.plugins.bcra_rem.normalizer import NormalizerBCRAREM

register_parser(ParserBCRAREM())
register_normalizer(NormalizerBCRAREM())

