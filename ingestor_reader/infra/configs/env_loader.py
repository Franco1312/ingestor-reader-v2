"""Environment variables loader."""
import os
from pathlib import Path
from dotenv import load_dotenv


def load_env_file() -> None:
    """Load environment variables from .env file if it exists."""
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if env_path.exists():
        # override=True ensures .env values take precedence over existing env vars
        load_dotenv(env_path, override=True)
    else:
        # Try loading from current directory
        load_dotenv(override=True)

