"""Configuration loader."""
import yaml
from pathlib import Path
from typing import Optional

from ingestor_reader.domain.entities.dataset_config import DatasetConfig


def load_config(dataset_id: str, config_path: Optional[str] = None) -> DatasetConfig:
    """
    Load dataset configuration from YAML.
    
    Args:
        dataset_id: Dataset identifier
        config_path: Optional path to config file. If None, looks in config/datasets/ directory.
                    In AWS deployments, config files must be included in the package.
        
    Returns:
        Validated DatasetConfig
        
    Raises:
        FileNotFoundError: If config file is not found
    """
    if config_path is None:
        # Try local config/datasets/ directory (works in local and AWS if files are in package)
        local_path = Path("config/datasets") / f"{dataset_id}.yml"
        if local_path.exists():
            config_path = str(local_path)
        else:
            # Try absolute path from package root (for AWS deployments)
            import ingestor_reader
            package_root = Path(ingestor_reader.__file__).parent.parent.parent
            absolute_path = package_root / "config" / "datasets" / f"{dataset_id}.yml"
            if absolute_path.exists():
                config_path = str(absolute_path)
            else:
                raise FileNotFoundError(
                    f"Config not found for dataset '{dataset_id}'. "
                    f"Tried: {local_path} and {absolute_path}. "
                    f"In AWS deployments, ensure config files are included in the package."
                )
    
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    # Handle list format (multiple datasets) - find by dataset_id
    if isinstance(data, list):
        dataset_data = None
        for item in data:
            if isinstance(item, dict) and item.get("dataset_id") == dataset_id:
                dataset_data = item
                break
        if dataset_data is None:
            raise ValueError(f"Dataset {dataset_id} not found in config file")
        data = dataset_data
    
    return DatasetConfig(**data)

