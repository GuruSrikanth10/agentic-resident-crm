import os
from dotenv import load_dotenv

# Automatically load the .env file if it exists
load_dotenv()

def get_required_env(var_name: str, default=None) -> str:
    value = os.environ.get(var_name, default)
    if value is None:
        raise ValueError(f"Required environment variable '{var_name}' is not set.")
    return value

def get_bool_env(var_name: str, default: bool = False) -> bool:
    value = os.environ.get(var_name)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")
