import json

import pytest
import requests

from iol_cli.iol_client import IOLAPIError, IOLClient


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.ok = 200 <= status < 300
        self.content = b"" if payload is None else json.dumps(payload).encode()
        self.text = "sensitive upstream body"
    def json(self): return self._payload


class Session:
    def __init__(self, responses): self.responses, self.calls = list(responses), []
    def _next(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception): raise result
        return result
    def post(self, url, **kwargs): return self._next("POST", url, **kwargs)
    def request(self, method, url, **kwargs): return self._next(method, url, **kwargs)


def client_with(responses): return IOLClient("u", "p", "https://iol.test", session=Session(responses))


def test_auth_and_request():
    client = client_with([Response(payload={"access_token": "secret", "refresh_token": "refresh", "expires_in": 900}), Response(payload={"ok": True})])
    assert client.get_account_status() == {"ok": True}
    assert client.session.calls[-1][2]["headers"]["Authorization"] == "Bearer secret"


def test_401_authenticates_once_again():
    token = Response(payload={"access_token": "one", "expires_in": 900})
    client = client_with([token, Response(401), Response(payload={"access_token": "two", "expires_in": 900}), Response(payload={"ok": True})])
    assert client.get_account_status() == {"ok": True}
    assert len(client.session.calls) == 4


@pytest.mark.parametrize("status", [429, 500])
def test_error_is_sanitized(status):
    client = client_with([Response(payload={"access_token": "secret"}), Response(status, {"password": "leak"})])
    with pytest.raises(IOLAPIError) as error: client.get_account_status()
    assert str(error.value) == f"HTTP {status}"
    assert "leak" not in str(error.value)


def test_timeout_is_sanitized():
    client = client_with([requests.Timeout("secret timeout details")])
    with pytest.raises(IOLAPIError, match="Timeout") as error: client.authenticate()
    assert "secret timeout details" not in str(error.value)
