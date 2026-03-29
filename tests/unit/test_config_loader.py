from pathlib import Path
import shutil
from uuid import uuid4

from src.common import config_loader
from src.common.config_loader import load_config, load_environment_config


def _temporary_test_dir() -> Path:
    test_dir = Path.cwd() / f"_config_loader_test_{uuid4().hex}"
    test_dir.mkdir()
    return test_dir


def test_load_config_reads_yaml_mapping() -> None:
    temp_dir = _temporary_test_dir()
    try:
        config_path = temp_dir / "dev.yaml"
        config_path.write_text("environment: dev\nsource:\n  page_size: 1000\n", encoding="utf-8")

        config = load_config(config_path)

        assert config["environment"] == "dev"
        assert config["source"]["page_size"] == 1000
    finally:
        shutil.rmtree(temp_dir)


def test_load_environment_config_uses_named_file() -> None:
    config_dir = _temporary_test_dir()
    try:
        (config_dir / "prod.yaml").write_text("environment: prod\n", encoding="utf-8")

        config = load_environment_config("prod", config_dir=config_dir)

        assert config["environment"] == "prod"
    finally:
        shutil.rmtree(config_dir)


def test_load_config_works_without_pyyaml(monkeypatch) -> None:
    temp_dir = _temporary_test_dir()
    try:
        config_path = temp_dir / "dev.yaml"
        config_path.write_text(
            "environment: dev\nsource:\n  page_size: 1000\n  base_url: \"https://example.test\"\n",
            encoding="utf-8",
        )

        monkeypatch.setattr(config_loader, "yaml", None)

        config = config_loader.load_config(config_path)

        assert config["environment"] == "dev"
        assert config["source"]["page_size"] == 1000
        assert config["source"]["base_url"] == "https://example.test"
    finally:
        shutil.rmtree(temp_dir)
