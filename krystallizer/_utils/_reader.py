from dataclasses import dataclass
from pathlib import Path
import json
import tomllib
import yaml


def _read_toml(path: str | Path) -> dict:
    """Convert a TOML file to a dictionary."""
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Error decoding TOML file: {e}")


def _read_yaml(path: str | Path) -> dict:
    """Convert a YAML file to a dictionary."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error decoding YAML file: {e}")


def _read_json(path: str | Path) -> dict:
    """Convert a JSON file to a dictionary."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON file: {e}")


class _ConfigReader:
    """Read a file and return a dictionary."""

    def __pre__init__(self, path: Path | str) -> None:
        if not isinstance(path, (Path, str)):
            raise TypeError("Path must be a string or a pathlib.Path object.")

    def __init__(self, path: Path | str) -> None:
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
