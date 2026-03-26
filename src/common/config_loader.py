"""Helpers for loading environment configuration files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return a dictionary payload."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"Config file must contain a mapping: {config_path}")

    return payload


def load_environment_config(
    environment: str,
    config_dir: str | Path = DEFAULT_CONFIG_DIR,
) -> dict[str, Any]:
    """Load a named environment configuration such as `dev` or `prod`."""
    return load_config(Path(config_dir) / f"{environment}.yaml")
