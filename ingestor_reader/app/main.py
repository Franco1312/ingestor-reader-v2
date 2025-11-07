"""CLI entry point."""
import logging
import typer

# Load environment variables from .env file first
from ingestor_reader.infra.configs.env_loader import load_env_file
load_env_file()

# Import plugins to register them
import ingestor_reader.infra.plugins  # noqa: F401

from ingestor_reader.infra.configs.config_loader import load_config
from ingestor_reader.infra.configs.app_config_loader import load_app_config
from ingestor_reader.use_cases.run_pipeline import run_pipeline

logger = logging.getLogger(__name__)


def run(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    full_reload: bool = typer.Option(False, "--full-reload", help="Force full reload even if source unchanged"),
):
    """Run ETL pipeline for a dataset."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Load application configuration
    app_config = load_app_config()
    
    # Load dataset configuration
    dataset_config = load_config(dataset_id)
    
    # Run pipeline
    run_result = run_pipeline(
        config=dataset_config,
        app_config=app_config,
        run_id=None,
        full_reload=full_reload,
    )
    
    typer.echo(f"Pipeline completed: run_id={run_result.run_id}, version={run_result.version_ts}")


if __name__ == "__main__":
    typer.run(run)

