"""INDEC IPC plugin."""
from ingestor_reader.infra.plugins.registry import register_parser, register_normalizer
from ingestor_reader.infra.plugins.indec_ipc.parser import ParserINDECIPC
from ingestor_reader.infra.plugins.generic.normalizer import GenericNormalizer

register_parser(ParserINDECIPC())
register_normalizer(GenericNormalizer("indec_ipc"))

