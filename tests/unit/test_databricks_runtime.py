from src.common.constants import BRONZE_TABLE, GOLD_TABLES, SILVER_REFERENCE_TABLES, SILVER_TABLE
import pytest

from src.common import databricks_runtime as runtime
from src.common.databricks_runtime import (
    build_abfss_uri,
    configure_adls_service_principal_access,
    resolve_runtime_config,
    validate_catalog_access,
    write_delta_table,
)


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


def test_resolve_runtime_config_applies_ingestion_contract_overrides() -> None:
    config = resolve_runtime_config(
        "dev",
        overrides={
            "run_date": "2024-01-02",
            "ingestion_mode": "adf_landed_raw",
            "raw_landing_path": "abfss://raw@storage.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=2024-01-02/",
            "batch_id": "adf-202401020115",
            "window_start": "2024-01-02T00:00:00",
            "window_end": "2024-01-03T00:00:00",
        },
    )

    assert config["runtime"] == {
        "run_date": "2024-01-02",
        "ingestion_mode": "adf_landed_raw",
        "raw_landing_path": "abfss://raw@storage.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=2024-01-02/",
        "batch_id": "adf-202401020115",
        "window_start": "2024-01-02T00:00:00",
        "window_end": "2024-01-03T00:00:00",
    }


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


def test_bootstrap_notebook_can_skip_storage_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
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
        configure_storage_access=False,
    )

    assert calls["configured_adls"] is False
    assert calls["ensured_catalog"] is True
    assert spark.conf.settings["spark.sql.session.timeZone"] == "UTC"
    assert config["environment"] == "dev"


def test_write_delta_table_writes_to_path_and_registers_matching_table() -> None:
    operations: list[tuple[str, object]] = []

    class FakeWriter:
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

        def save(self, path: str) -> None:
            operations.append(("save", path))

    class FakeSqlResult:
        def __init__(self, rows: list[dict[str, str]]) -> None:
            self._rows = rows

        def collect(self) -> list[dict[str, str]]:
            return self._rows

    class FakeSpark:
        def sql(self, query: str) -> FakeSqlResult:
            operations.append(("sql", query))
            if query.startswith("DESCRIBE DETAIL"):
                return FakeSqlResult(
                    [{"location": "abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw"}]
                )
            return FakeSqlResult([])

    class FakeDataFrame:
        def __init__(self) -> None:
            self.sparkSession = FakeSpark()

        @property
        def write(self) -> FakeWriter:
            return FakeWriter()

    write_delta_table(
        FakeDataFrame(),
        "bronze.nyc311_service_requests_raw",
        "abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw",
        mode="overwrite",
    )

    save_calls = [value for key, value in operations if key == "save"]
    sql_calls = [value for key, value in operations if key == "sql"]

    assert save_calls == ["abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw"]
    assert sql_calls == [
        "CREATE TABLE IF NOT EXISTS bronze.nyc311_service_requests_raw USING DELTA LOCATION 'abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw'",
        "DESCRIBE DETAIL bronze.nyc311_service_requests_raw",
    ]


def test_write_delta_table_raises_when_registered_location_does_not_match() -> None:
    class FakeWriter:
        def format(self, value: str) -> "FakeWriter":
            return self

        def mode(self, value: str) -> "FakeWriter":
            return self

        def option(self, key: str, value: str) -> "FakeWriter":
            return self

        def save(self, path: str) -> None:
            return None

    class FakeSqlResult:
        def __init__(self, rows: list[dict[str, str]]) -> None:
            self._rows = rows

        def collect(self) -> list[dict[str, str]]:
            return self._rows

    class FakeSpark:
        def sql(self, query: str) -> FakeSqlResult:
            if query.startswith("DESCRIBE DETAIL"):
                return FakeSqlResult([{"location": "dbfs:/user/hive/warehouse/bronze.db/nyc311_service_requests_raw"}])
            return FakeSqlResult([])

    class FakeDataFrame:
        def __init__(self) -> None:
            self.sparkSession = FakeSpark()

        @property
        def write(self) -> FakeWriter:
            return FakeWriter()

    with pytest.raises(ValueError, match="expected abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw"):
        write_delta_table(
            FakeDataFrame(),
            "bronze.nyc311_service_requests_raw",
            "abfss://nyc311@account.dfs.core.windows.net/bronze/nyc311_service_requests_raw",
            mode="overwrite",
        )


def test_configure_adls_service_principal_access_skips_unavailable_serverless_configs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeSparkConf:
        def set(self, key: str, value: str) -> None:
            raise Exception(
                "[CONFIG_NOT_AVAILABLE] Configuration "
                f"{key} is not available."
            )

    class FakeSpark:
        def __init__(self) -> None:
            self.conf = FakeSparkConf()

    class FakeSecrets:
        def get(self, scope: str, key: str) -> str:
            raise AssertionError(f"Secrets should not be requested when Spark config is unavailable: {scope}/{key}")

    class FakeDbutils:
        def __init__(self) -> None:
            self.secrets = FakeSecrets()

    configure_adls_service_principal_access(
        spark=FakeSpark(),
        dbutils=FakeDbutils(),
        storage_account="nyc311adlsg22026",
        secret_scope="adls-sp",
        client_id_key="client-id",
        client_secret_key="client-secret",
        tenant_id_key="tenant-id",
    )

    assert "continuing with workspace-managed access" in capsys.readouterr().out
