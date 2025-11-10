"""CLI entry point for local development."""
import typer

import ingestor_reader.infra.plugins

from ingestor_reader.infra.common import load_app_config, load_dataset_config, setup_logging, get_logger
from ingestor_reader.use_cases.run_pipeline import run_pipeline

setup_logging()
logger = get_logger(__name__)


def run(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    full_reload: bool = typer.Option(False, "--full-reload", help="Force full reload even if source unchanged"),
):
    """Run ETL pipeline for a dataset (local development)."""
    app_config = load_app_config()
    dataset_config = load_dataset_config(dataset_id)
    
    run_result = run_pipeline(
        config=dataset_config,
        app_config=app_config,
        run_id=None,
        full_reload=full_reload,
    )
    
    typer.echo(f"Pipeline completed: run_id={run_result.run_id}, version={run_result.version_ts}")


if __name__ == "__main__":
    typer.run(run)

