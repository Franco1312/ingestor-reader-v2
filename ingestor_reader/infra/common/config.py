"""Centralized configuration loading."""
import os
import yaml
import importlib
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

from ingestor_reader.domain.entities.app_config import AppConfig
from ingestor_reader.domain.entities.dataset_config import DatasetConfig


def _load_env_file() -> None:
    """Load environment variables from .env file if it exists."""
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME") or os.getenv("ECS_CONTAINER_METADATA_URI"):
        return
    
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        load_dotenv(override=True)


def load_app_config(env: Optional[str] = None) -> AppConfig:
    """
    Load application configuration for environment.
    
    Args:
        env: Environment name (local, staging, production). 
             If None, reads from ENV environment variable.
        
    Returns:
        AppConfig instance
        
    Raises:
        ConfigError: If config module not found or invalid
    """
    _load_env_file()
    
    if env is None:
        env = os.getenv("ENV", "local")
    
    if env not in ("local", "staging", "production"):
        raise ConfigError(f"Invalid environment: {env}. Must be one of: local, staging, production")
    
    module_name = f"config.appconfig.{env}"
    try:
        config_module = importlib.import_module(module_name)
        return config_module.config
    except ImportError as e:
        raise ConfigError(f"Config module not found: {module_name}") from e


def load_dataset_config(dataset_id: str, config_path: Optional[str] = None) -> DatasetConfig:
    """
    Load dataset configuration from YAML.
    
    Args:
        dataset_id: Dataset identifier
        config_path: Optional path to config file. If None, looks in config/datasets/ directory.
        
    Returns:
        Validated DatasetConfig
        
    Raises:
        ConfigError: If config file is not found or invalid
    """
    if config_path is None:
        local_path = Path("config/datasets") / f"{dataset_id}.yml"
        if local_path.exists():
            config_path = str(local_path)
        else:
            import ingestor_reader
            package_root = Path(ingestor_reader.__file__).parent.parent.parent
            absolute_path = package_root / "config" / "datasets" / f"{dataset_id}.yml"
            if absolute_path.exists():
                config_path = str(absolute_path)
            else:
                raise ConfigError(
                    f"Config not found for dataset '{dataset_id}'. "
                    f"Tried: {local_path} and {absolute_path}."
                )
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise ConfigError(f"Config file not found: {config_path}") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in config file: {config_path}") from e
    
    if isinstance(data, list):
        dataset_data = None
        for item in data:
            if isinstance(item, dict) and item.get("dataset_id") == dataset_id:
                dataset_data = item
                break
        if dataset_data is None:
            raise ConfigError(f"Dataset {dataset_id} not found in config file")
        data = dataset_data
    
    try:
        return DatasetConfig(**data)
    except Exception as e:
        raise ConfigError(f"Invalid dataset config: {e}") from e

