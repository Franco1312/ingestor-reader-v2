"""Generic plugins."""
from ingestor_reader.infra.plugins.generic.normalizer import GenericNormalizer
from ingestor_reader.infra.plugins.registry import register_normalizer

register_normalizer(GenericNormalizer("generic"))

