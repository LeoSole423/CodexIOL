import pytest


@pytest.fixture
def cli_env(monkeypatch, tmp_path):
    monkeypatch.setenv("IOL_USERNAME", "test-user")
    monkeypatch.setenv("IOL_PASSWORD", "test-password")
    monkeypatch.setenv("IOL_API_URL", "https://example.invalid")
    monkeypatch.setenv("IOL_STATE_DIR", str(tmp_path))
    return tmp_path
