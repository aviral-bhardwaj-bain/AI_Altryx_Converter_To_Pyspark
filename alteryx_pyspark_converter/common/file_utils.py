"""File utility functions for the converter."""

import os
import json
import logging
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def ensure_dir(path: str) -> str:
    """Ensure a directory exists, creating it if necessary."""
    os.makedirs(path, exist_ok=True)
    return path


def read_json(path: str) -> dict:
    """Read a JSON file and return its contents."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(data: Any, path: str, indent: int = 2) -> str:
    """Write data to a JSON file."""
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
    return path


def read_yaml(path: str) -> dict:
    """Read a YAML file and return its contents."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_yaml(data: Any, path: str) -> str:
    """Write data to a YAML file."""
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
    return path


def read_text(path: str) -> str:
    """Read a text file."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(content: str, path: str) -> str:
    """Write text to a file."""
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path
