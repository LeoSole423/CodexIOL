import json
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class IOLAPIError(RuntimeError):
    pass

class IOLClient:
    def __init__(self, username: str, password: str, base_url: str, timeout: int = 20):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._create_session()
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "PATCH"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def authenticate(self) -> None:
        url = f"{self.base_url}/token"
        data = {
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }
        resp = self.session.post(url, data=data, timeout=self.timeout)
        if resp.status_code != 200:
            raise IOLAPIError(f"Auth failed: {resp.status_code} - {resp.text}")
        token = resp.json()
        self.access_token = token.get("access_token")
        self.refresh_token = token.get("refresh_token")
        expires_in = token.get("expires_in", 3600)
        self.token_expiry = time.time() + max(0, int(expires_in) - 60)

    def refresh(self) -> None:
        if not self.refresh_token:
            self.authenticate()
            return
        url = f"{self.base_url}/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        resp = self.session.post(url, data=data, timeout=self.timeout)
        if resp.status_code != 200:
            self.authenticate()
            return
        token = resp.json()
        self.access_token = token.get("access_token")
        self.refresh_token = token.get("refresh_token")
        expires_in = token.get("expires_in", 3600)
        self.token_expiry = time.time() + max(0, int(expires_in) - 60)

    def _ensure_token(self) -> None:
        if not self.access_token or time.time() >= self.token_expiry:
            self.refresh()

    def _headers(self) -> Dict[str, str]:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                 payload: Optional[Dict[str, Any]] = None, raw_json: Optional[str] = None) -> Any:
        url = f"{self.base_url}{path}"
        headers = self._headers()
        data = None
        json_payload = None
        if raw_json is not None:
            data = raw_json
            headers["Content-Type"] = "application/json"
        else:
            json_payload = payload
        resp = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_payload,
            data=data,
            timeout=self.timeout,
        )
        if resp.status_code == 401:
            self.authenticate()
            headers = self._headers()
            resp = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
                data=data,
                timeout=self.timeout,
            )
        if not resp.ok:
            raise IOLAPIError(f"HTTP {resp.status_code}: {resp.text}")
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return resp.json()
        try:
            return resp.json()
        except ValueError:
            return resp.text

    def get_portfolio(self, country: str) -> Any:
        return self._request("GET", f"/api/v2/portafolio/{country}")

    def get_account_status(self) -> Any:
        return self._request("GET", "/api/v2/estadocuenta")

    def get_quote(self, market: str, symbol: str) -> Any:
        return self._request("GET", f"/api/v2/{market}/Titulos/{symbol}/Cotizacion")

    def get_instruments(self, country: str) -> Any:
        return self._request("GET", f"/api/v2/{country}/Titulos/Cotizacion/Instrumentos")

    def get_panels(self, country: str, instrument: str) -> Any:
        return self._request("GET", f"/api/v2/{country}/Titulos/Cotizacion/Paneles/{instrument}")

    def get_panel_quotes(self, instrumento: str, panel: str, pais: str) -> Any:
        return self._request("GET", f"/api/v2/Cotizaciones/{instrumento}/{panel}/{pais}")

    def list_orders(self, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", "/api/v2/operaciones", params=params)

    def get_order(self, numero: int) -> Any:
        return self._request("GET", f"/api/v2/operaciones/{numero}")

    def cancel_order(self, numero: int) -> Any:
        return self._request("DELETE", f"/api/v2/operaciones/{numero}")

    def buy(self, payload: Dict[str, Any], especie_d: bool = False) -> Any:
        path = "/api/v2/operar/ComprarEspecieD" if especie_d else "/api/v2/operar/Comprar"
        return self._request("POST", path, payload=payload)

    def sell(self, payload: Dict[str, Any], especie_d: bool = False) -> Any:
        path = "/api/v2/operar/VenderEspecieD" if especie_d else "/api/v2/operar/Vender"
        return self._request("POST", path, payload=payload)

    def fci_subscribe(self, payload: Dict[str, Any]) -> Any:
        return self._request("POST", "/api/v2/operar/suscripcion/fci", payload=payload)

    def fci_redeem(self, payload: Dict[str, Any]) -> Any:
        return self._request("POST", "/api/v2/operar/rescate/fci", payload=payload)

    def raw_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                    payload: Optional[Dict[str, Any]] = None, raw_json: Optional[str] = None) -> Any:
        return self._request(method, path, params=params, payload=payload, raw_json=raw_json)
