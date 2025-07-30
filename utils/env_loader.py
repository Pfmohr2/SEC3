# utils/env_loader.py
import os
from pathlib import Path
from dotenv import load_dotenv

DEPLOY_ENV = os.environ["DEPLOY_ENV"]

if DEPLOY_ENV == "dev":
    filename = "params.dev.env"
elif DEPLOY_ENV == "qa":
    filename = "params.qa.env"
elif DEPLOY_ENV == "prod":
    filename = "params.prod.env"
else:
    filename = "params.dev.env"


def load_env_file(filename=filename, levels_up=2):
    base = Path(__file__).resolve()
    for _ in range(levels_up):
        base = base.parent
    env_path = base / filename
    if not env_path.exists():
        raise FileNotFoundError(f".env not found at: {env_path}")
    load_dotenv(dotenv_path=env_path, override=True)