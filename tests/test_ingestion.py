from ingestion.ingestion import build_jdbc_url


def test_jdbc_url_uses_server_database_and_encryption():
    cfg = {"sql": {"server": "myserver.database.windows.net", "database": "shoplogs"}}
    url = build_jdbc_url(cfg)
    assert url.startswith("jdbc:sqlserver://myserver.database.windows.net:1433;")
    assert "database=shoplogs;" in url
    assert "encrypt=true;" in url