"""AWS Lambda handler."""
import json
from typing import Any

import ingestor_reader.infra.plugins

from ingestor_reader.infra.common import (
    load_app_config,
    load_dataset_config,
    setup_logging,
    get_logger,
)
from ingestor_reader.use_cases.run_pipeline import run_pipeline

setup_logging(force=True)
logger = get_logger(__name__)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda handler for ETL pipeline.
    
    Expected event format:
    {
        "dataset_id": "bcra_infomondia_series",
        "full_reload": false  # optional, defaults to false
    }
    
    Environment variables required:
    - S3_BUCKET: S3 bucket name
    - AWS_REGION: AWS region (optional, defaults to us-east-1)
    - SNS_TOPIC_ARN: SNS topic ARN for notifications (optional)
    - ENV: Environment name (local, staging, production) - optional, defaults to local
    
    Returns:
        {
            "statusCode": 200,
            "body": {
                "dataset_id": "...",
                "run_id": "...",
                "version_ts": "...",
                "status": "completed"
            }
        }
    """
    try:

        dataset_id = event.get("dataset_id")
        if not dataset_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "dataset_id is required"}),
            }
        
        full_reload = event.get("full_reload", False)
        
        logger.info("Starting pipeline for dataset: %s", dataset_id)
        
        app_config = load_app_config()
        logger.info("Configuration loaded: verify_ssl=%s, s3_bucket=%s", app_config.verify_ssl, app_config.s3_bucket)
        dataset_config = load_dataset_config(dataset_id)
        

        run_result = run_pipeline(
            config=dataset_config,
            app_config=app_config,
            run_id=None,
            full_reload=full_reload,
        )
        
        result = {
            "dataset_id": run_result.dataset_id,
            "run_id": run_result.run_id,
            "version_ts": run_result.version_ts,
            "status": "completed",
        }
        
        logger.info("Pipeline completed: %s", json.dumps(result))
        
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
        
    except FileNotFoundError as e:
        logger.error("Config not found: %s", e)
        return {
            "statusCode": 404,
            "body": json.dumps({"error": str(e)}),
        }
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

