"""Environment variables loader."""
import os
from pathlib import Path
from dotenv import load_dotenv


def load_env_file() -> None:
    """
    Load environment variables from .env file if it exists.
    
    In AWS deployments, environment variables should be set directly
    (via Lambda environment, ECS task definition, etc.) and .env files are not used.
    This function is primarily for local development.
    """


    if os.getenv("AWS_LAMBDA_FUNCTION_NAME") or os.getenv("ECS_CONTAINER_METADATA_URI"):
        return
    

    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:

        load_dotenv(override=True)

