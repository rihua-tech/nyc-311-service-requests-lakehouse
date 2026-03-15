from pathlib import Path

from src.common.config_loader import load_config, load_environment_config


def test_load_config_reads_yaml_mapping(tmp_path: Path) -> None:
    config_path = tmp_path / "dev.yaml"
    config_path.write_text("environment: dev\nsource:\n  page_size: 1000\n", encoding="utf-8")

    config = load_config(config_path)

    assert config["environment"] == "dev"
    assert config["source"]["page_size"] == 1000


def test_load_environment_config_uses_named_file(tmp_path: Path) -> None:
    (tmp_path / "prod.yaml").write_text("environment: prod\n", encoding="utf-8")

    config = load_environment_config("prod", config_dir=tmp_path)

    assert config["environment"] == "prod"

