"""Helpers for loading environment configuration files."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - exercised via fallback tests
    yaml = None

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _strip_yaml_comment(line: str) -> str:
    in_single_quote = False
    in_double_quote = False
    escaped = False

    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            continue
        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            continue
        if char == "#" and not in_single_quote and not in_double_quote:
            return line[:index]

    return line


def _parse_yaml_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"null", "none", "~"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        pass

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    return value


def _load_simple_yaml(text: str, source: Path) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = _strip_yaml_comment(raw_line).rstrip()
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip(" "))
        if indent % 2 != 0:
            raise ValueError(f"Unsupported indentation in config file at {source}:{line_number}")

        stripped = line.lstrip(" ")
        if stripped.startswith("- "):
            raise ValueError(f"YAML lists are not supported by the fallback parser: {source}:{line_number}")
        if ":" not in stripped:
            raise ValueError(f"Invalid YAML mapping entry in {source}:{line_number}")

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        value = raw_value.strip()

        while indent <= stack[-1][0] and len(stack) > 1:
            stack.pop()
        current = stack[-1][1]

        if value == "":
            child: dict[str, Any] = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = _parse_yaml_scalar(value)

    return root


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return a dictionary payload."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config_text = config_path.read_text(encoding="utf-8")
    if yaml is not None:
        payload = yaml.safe_load(config_text) or {}
    else:
        payload = _load_simple_yaml(config_text, config_path)

    if not isinstance(payload, dict):
        raise ValueError(f"Config file must contain a mapping: {config_path}")

    return payload


def load_environment_config(
    environment: str,
    config_dir: str | Path = DEFAULT_CONFIG_DIR,
) -> dict[str, Any]:
    """Load a named environment configuration such as `dev` or `prod`."""
    return load_config(Path(config_dir) / f"{environment}.yaml")
