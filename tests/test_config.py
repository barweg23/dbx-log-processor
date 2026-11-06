import config


def test_every_source_has_the_required_keys():
    cfg = config.load_config()
    for source in cfg["sources"]:
        assert "name" in source
        assert "table" in source
        assert "watermark" in source
        assert "rules" in source


def test_bundle_variable_overrides_catalog(monkeypatch):
    monkeypatch.setenv("LOGS_CATALOG", "logs_prod")
    cfg = config.load_config()
    assert cfg["catalog"] == "logs_prod"


def test_table_name():
    cfg = {"catalog": "logs"}
    assert config.table_name(cfg, "bronze", "web") == "logs.bronze.logs_web"


def test_raw_path():
    cfg = {"storage": {"account": "myaccount", "raw_container": "raw"}}
    assert config.raw_path(cfg, "web") == "abfss://raw@myaccount.dfs.core.windows.net/web/"


def test_checkpoint_path():
    cfg = {"storage": {"account": "myaccount", "processed_container": "processed",
                       "checkpoints_folder": "checkpoints"}}
    expected = "abfss://processed@myaccount.dfs.core.windows.net/checkpoints/web/"
    assert config.checkpoint_path(cfg, "web") == expected