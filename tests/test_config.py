"""Unit tests for the shared configuration module (pipelines/common/config.py).

The catalog resolution order is FOOTBALL_CATALOG env var > football.catalog
pipeline config > football_col_dev default; for_catalog() re-derives every
table/volume name for any catalog (what the notebook shim and the prod target
rely on).
"""

from common import config


def test_default_catalog_fallback(monkeypatch):
    monkeypatch.delenv("FOOTBALL_CATALOG", raising=False)
    assert config.resolve_catalog() == config.DEFAULT_CATALOG


def test_env_var_overrides(monkeypatch):
    monkeypatch.setenv("FOOTBALL_CATALOG", "football_col_test")
    assert config.resolve_catalog() == "football_col_test"


def test_for_catalog_names():
    cfg = config.for_catalog("football_col_prod")
    assert cfg.CATALOG == "football_col_prod"
    assert cfg.BRONZE_TABLE == "football_col_prod.bronze.api_fixtures"
    assert cfg.SILVER_TABLE == "football_col_prod.silver.api_fixtures"
    assert cfg.GOLD_SCHEMA == "football_col_prod.gold"
    assert cfg.GOLD_DIM_TEAM == "football_col_prod.gold.dim_team"
    assert cfg.FIXTURES_DIR == "/Volumes/football_col_prod/landing/raw/api_football/fixtures"


def test_module_constants_follow_default_catalog():
    # No env var and no pipeline conf in pytest: constants resolve to dev.
    assert config.CATALOG == config.DEFAULT_CATALOG
    assert config.BRONZE_TABLE == f"{config.DEFAULT_CATALOG}.bronze.api_fixtures"
