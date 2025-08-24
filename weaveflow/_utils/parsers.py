import json
import tomllib
from collections.abc import Callable
from pathlib import Path

import yaml


def _read_toml(path: str | Path) -> dict:
    """Convert a TOML file to a dictionary."""
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {path}") from e
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Error decoding TOML file: {e}") from e


def _read_yaml(path: str | Path) -> dict:
    """Convert a YAML file to a dictionary."""
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {path}") from e
    except yaml.YAMLError as e:
        raise ValueError(f"Error decoding YAML file: {e}") from e


def _read_json(path: str | Path) -> dict:
    """Convert a JSON file to a dictionary."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {path}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON file: {e}") from e


class _ConfigReader:
    """Read a file and return a dictionary."""

    def __pre__init__(self, path: Path | str) -> None:
        if not isinstance(path, (Path, str)):
            raise TypeError("Path must be a string or a pathlib.Path object.")

    @staticmethod
    def _extend_engines(
        default_engine: dict[str, callable], custom_engine: dict[str, callable]
    ) -> None:
        """Extend the default engine with custom engines."""
        for ext, reader in custom_engine.items():
            if not isinstance(ext, str):
                raise TypeError(f"Extension must be a string, got {ext}.")
            if not isinstance(reader, Callable):
                raise TypeError(f"Reader must be a callable, got {reader}.")

            if not ext.startswith("."):
                ext = f".{ext}"

            default_engine[ext.lower()] = reader

        return default_engine

    def __init__(
        self, path: Path | str, custom_engine: dict[str, callable] | None = None
    ) -> None:
        self.path = path
        self.extension = Path(path).suffix.lower()
        _engines = {
            ".toml": _read_toml,
            ".yaml": _read_yaml,
            ".yml": _read_yaml,
            ".json": _read_json,
        }
        if custom_engine is not None:
            _engines = self._extend_engines(_engines, custom_engine)

        self._engine = _engines[self.extension]

    def read(self) -> dict:
        """Read a file and return a dictionary."""
        return self._engine(self.path)
