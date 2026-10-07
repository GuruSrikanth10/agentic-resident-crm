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

def split_names(value: str) -> tuple:
    """`"a, b,,a"` -> `("a", "b")`: comma-separated names, blanks dropped,
    order kept, repeats removed. Kafka topic names cannot contain a comma, so
    a topic setting can carry a list this way and a single name is unchanged."""
    return tuple(dict.fromkeys(name.strip() for name in (value or "").split(",")
                               if name.strip()))
