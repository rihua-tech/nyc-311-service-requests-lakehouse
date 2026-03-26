from src.common.constants import BRONZE_TABLE, GOLD_TABLES, SILVER_REFERENCE_TABLES, SILVER_TABLE
import pytest

from src.common import databricks_runtime as runtime
from src.common.databricks_runtime import build_abfss_uri, resolve_runtime_config, validate_catalog_access, write_delta_table


def test_build_abfss_uri_joins_parts_cleanly() -> None:
    uri = build_abfss_uri("nyc311", "nyc311adlsg22026", "bronze", "service_requests")

    assert uri == "abfss://nyc311@nyc311adlsg22026.dfs.core.windows.net/bronze/service_requests"


def test_resolve_runtime_config_derives_manual_cloud_paths() -> None:
    config = resolve_runtime_config("dev", overrides={"catalog": "workspace_catalog"})

    assert config["azure"]["storage_account"] == "nyc311adlsg22026"
    assert config["azure"]["container"] == "nyc311"
    assert config["databricks"]["catalog"] == "workspace_catalog"
    assert config["paths"]["bronze_base_path"].endswith("/bronze")
    assert config["paths"]["silver_base_path"].endswith("/silver")
    assert config["paths"]["gold_base_path"].endswith("/gold")
    assert config["source"]["watermark_state_path"].startswith("abfss://nyc311@nyc311adlsg22026")
    assert config["paths"]["table_paths"][BRONZE_TABLE].endswith("/nyc311_service_requests_raw")
    assert config["paths"]["table_paths"][SILVER_TABLE].endswith("/service_requests_clean")

    for table_name in SILVER_REFERENCE_TABLES.values():
        assert config["paths"]["table_paths"][table_name].startswith(config["paths"]["silver_base_path"])

    for table_name in GOLD_TABLES.values():
        assert config["paths"]["table_paths"][table_name].startswith(config["paths"]["gold_base_path"])


def test_validate_catalog_access_raises_clear_error_for_missing_catalog() -> None:
    class FakeSpark:
        class _Result:
            def collect(self) -> list[tuple[str]]:
                return [("dev_catalog",), ("analytics",)]

        def sql(self, query: str) -> "FakeSpark._Result":
            if query == "SHOW CATALOGS":
                return self._Result()
            raise AssertionError(f"Unexpected query: {query}")

    with pytest.raises(ValueError, match="Set databricks.catalog"):
        validate_catalog_access(
            FakeSpark(),
            {
                "environment": "dev",
                "databricks": {
                    "catalog": "",
                },
            },
        )


def test_validate_catalog_access_auto_selects_single_non_system_catalog() -> None:
    class FakeSpark:
        class _CatalogsResult:
            def collect(self) -> list[tuple[str]]:
                return [("dbw_nyc311_lakehouse_central",), ("samples",), ("system",)]

        class _UseCatalogResult:
            def collect(self) -> list[tuple[str]]:
                return []

        def sql(self, query: str) -> "FakeSpark._CatalogsResult | FakeSpark._UseCatalogResult":
            if query == "SHOW CATALOGS":
                return self._CatalogsResult()
            if query == "USE CATALOG `dbw_nyc311_lakehouse_central`":
                return self._UseCatalogResult()
            raise AssertionError(f"Unexpected query: {query}")

    config = {
        "environment": "dev",
        "databricks": {
            "catalog": "",
        },
    }

    resolved_catalog = validate_catalog_access(FakeSpark(), config)

    assert resolved_catalog == "dbw_nyc311_lakehouse_central"
    assert config["databricks"]["catalog"] == "dbw_nyc311_lakehouse_central"


def test_bootstrap_notebook_can_skip_catalog_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {
        "configured_adls": False,
        "ensured_catalog": False,
    }

    class FakeSparkConf:
        def __init__(self) -> None:
            self.settings: dict[str, str] = {}

        def set(self, key: str, value: str) -> None:
            self.settings[key] = value

    class FakeSpark:
        def __init__(self) -> None:
            self.conf = FakeSparkConf()

    class FakeWidgets:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def get(self, name: str) -> str:
            if name not in self.values:
                raise KeyError(name)
            return self.values[name]

        def text(self, name: str, default: str) -> None:
            self.values[name] = default

    class FakeDbutils:
        def __init__(self) -> None:
            self.widgets = FakeWidgets()

    def fake_configure_adls_service_principal_access(**_: object) -> None:
        calls["configured_adls"] = True

    def fake_ensure_catalog_and_schemas(*_: object, **__: object) -> None:
        calls["ensured_catalog"] = True

    monkeypatch.setattr(runtime, "configure_adls_service_principal_access", fake_configure_adls_service_principal_access)
    monkeypatch.setattr(runtime, "ensure_catalog_and_schemas", fake_ensure_catalog_and_schemas)

    spark = FakeSpark()
    config = runtime.bootstrap_notebook(
        spark=spark,
        dbutils=FakeDbutils(),
        ensure_catalog_schemas=False,
    )

    assert calls["configured_adls"] is True
    assert calls["ensured_catalog"] is False
    assert spark.conf.settings["spark.sql.session.timeZone"] == "UTC"
    assert config["environment"] == "dev"


def test_write_delta_table_falls_back_to_managed_table_when_external_location_is_missing() -> None:
    operations: list[tuple[str, object]] = []

    class FakeWriter:
        def __init__(self, attempt: int) -> None:
            self.attempt = attempt

        def format(self, value: str) -> "FakeWriter":
            operations.append(("format", value))
            return self

        def mode(self, value: str) -> "FakeWriter":
            operations.append(("mode", value))
            return self

        def option(self, key: str, value: str) -> "FakeWriter":
            operations.append((f"option:{key}", value))
            return self

        def partitionBy(self, *values: str) -> "FakeWriter":
            operations.append(("partitionBy", values))
            return self

        def saveAsTable(self, table_name: str) -> None:
            operations.append(("saveAsTable", table_name))
            if self.attempt == 1:
                raise Exception(
                    "[NO_PARENT_EXTERNAL_LOCATION_FOR_PATH] No parent external location was found for path "
                    "'abfss://nyc311@account.dfs.core.windows.net/bronze/table'."
                )

    class FakeDataFrame:
        def __init__(self) -> None:
            self.attempt = 0

        @property
        def write(self) -> FakeWriter:
            self.attempt += 1
            return FakeWriter(self.attempt)

    write_delta_table(
        FakeDataFrame(),
        "bronze.nyc311_service_requests_raw",
        "abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw",
        mode="overwrite",
    )

    path_options = [value for key, value in operations if key == "option:path"]
    save_calls = [value for key, value in operations if key == "saveAsTable"]

    assert path_options == ["abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw"]
    assert save_calls == ["bronze.nyc311_service_requests_raw", "bronze.nyc311_service_requests_raw"]
