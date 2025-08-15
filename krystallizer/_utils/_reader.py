from dataclasses import dataclass
from pathlib import Path
import json
import tomllib
import yaml


def _read_toml(path: str) -> dict:
    """Convert a TOML file to a dictionary."""
    if not path.endswith(".toml"):
        raise ValueError("File must be a TOML file.")

    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Error decoding TOML file: {e}")


def _read_yaml(path: str) -> dict:
    """Convert a YAML file to a dictionary."""
    if not path.endswith(".yaml") and not path.endswith(".yml"):
        raise ValueError("File must be a YAML file.")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error decoding YAML file: {e}")


def _read_json(path: str) -> dict:
    """Convert a JSON file to a dictionary."""
    if not path.endswith(".json"):
        raise ValueError("File must be a JSON file.")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON file: {e}")


class _ConfigReader:
    """Read a file and return a dictionary."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.extension = Path(path).suffix.lower()
        _engines = {
            ".toml": _read_toml,
            ".yaml": _read_yaml,
            ".yml": _read_yaml,
            ".json": _read_json,
        }
        self._engine = _engines[self.extension]

    def read(self) -> dict:
        """Read a file and return a dictionary."""
        return self._engine(self.path)
