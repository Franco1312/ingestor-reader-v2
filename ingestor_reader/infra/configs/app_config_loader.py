"""Application configuration loader."""
import os
import importlib

from ingestor_reader.domain.entities.app_config import AppConfig


def load_app_config(env: str | None = None) -> AppConfig:
    """
    Load application configuration for environment.
    
    Args:
        env: Environment name (local, staging, production). 
             If None, reads from ENV environment variable.
        
    Returns:
        AppConfig instance
    """
    if env is None:
        env = os.getenv("ENV", "local")
    
    if env not in ("local", "staging", "production"):
        raise ValueError(f"Invalid environment: {env}. Must be one of: local, staging, production")
    

    module_name = f"config.appconfig.{env}"
    try:
        config_module = importlib.import_module(module_name)
        return config_module.config
    except ImportError as e:
        raise FileNotFoundError(f"Config module not found: {module_name}") from e

