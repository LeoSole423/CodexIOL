from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class IOLAPIError(RuntimeError):
    pass


class IOLClient:
    def __init__(self, username: str, password: str, base_url: str, timeout: int = 20, session: Optional[requests.Session] = None):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or self._create_session()
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry = 0.0

    @staticmethod
    def _create_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, connect=3, read=3, status=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}))
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @staticmethod
    def _error(response: requests.Response) -> IOLAPIError:
        request_id = response.headers.get("x-request-id") or response.headers.get("trace-id")
        suffix = f" (request id: {request_id})" if request_id else ""
        return IOLAPIError(f"HTTP {response.status_code}{suffix}")

    def authenticate(self) -> None:
        try:
            response = self.session.post(f"{self.base_url}/token", data={"username": self.username, "password": self.password, "grant_type": "password"}, timeout=self.timeout)
        except requests.RequestException as exc:
            raise IOLAPIError(f"authentication transport failure: {type(exc).__name__}") from exc
        if response.status_code != 200:
            raise self._error(response)
        token = response.json()
        self.access_token = token.get("access_token")
        self.refresh_token = token.get("refresh_token")
        if not self.access_token:
            raise IOLAPIError("authentication response did not contain an access token")
        self.token_expiry = time.time() + max(0, int(token.get("expires_in", 900)) - 60)

    def refresh(self) -> None:
        if not self.refresh_token:
            self.authenticate()
            return
        try:
            response = self.session.post(f"{self.base_url}/token", data={"grant_type": "refresh_token", "refresh_token": self.refresh_token}, timeout=self.timeout)
        except requests.RequestException:
            self.authenticate()
            return
        if response.status_code != 200:
            self.authenticate()
            return
        token = response.json()
        self.access_token = token.get("access_token")
        self.refresh_token = token.get("refresh_token", self.refresh_token)
        self.token_expiry = time.time() + max(0, int(token.get("expires_in", 900)) - 60)

    def _headers(self) -> Dict[str, str]:
        if not self.access_token or time.time() >= self.token_expiry:
            self.refresh()
        return {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, payload: Any = None) -> Any:
        if not path.startswith("/"):
            raise IOLAPIError("API path must start with /")
        try:
            response = self.session.request(method.upper(), f"{self.base_url}{path}", headers=self._headers(), params=params, json=payload, timeout=self.timeout)
            if response.status_code == 401:
                self.authenticate()
                response = self.session.request(method.upper(), f"{self.base_url}{path}", headers=self._headers(), params=params, json=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            raise IOLAPIError(f"transport failure: {type(exc).__name__}") from exc
        if not response.ok:
            raise self._error(response)
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    def get_portfolio(self, country: str) -> Any: return self._request("GET", f"/api/v2/portafolio/{country}")
    def get_account_status(self) -> Any: return self._request("GET", "/api/v2/estadocuenta")
    def get_quote(self, market: str, symbol: str) -> Any: return self._request("GET", f"/api/v2/{market}/Titulos/{symbol}/Cotizacion")
    def get_instruments(self, country: str) -> Any: return self._request("GET", f"/api/v2/{country}/Titulos/Cotizacion/Instrumentos")
    def get_panels(self, country: str, instrument: str) -> Any: return self._request("GET", f"/api/v2/{country}/Titulos/Cotizacion/Paneles/{instrument}")
    def get_panel_quotes(self, instrument: str, panel: str, country: str) -> Any: return self._request("GET", f"/api/v2/Cotizaciones/{instrument}/{panel}/{country}")
    def list_orders(self, params: Optional[Dict[str, Any]] = None) -> Any: return self._request("GET", "/api/v2/operaciones", params=params)
    def get_order(self, number: int) -> Any: return self._request("GET", f"/api/v2/operaciones/{number}")
    def cancel_order(self, number: int) -> Any: return self._request("DELETE", f"/api/v2/operaciones/{number}")
    def buy(self, payload: dict, especie_d: bool = False) -> Any: return self._request("POST", "/api/v2/operar/ComprarEspecieD" if especie_d else "/api/v2/operar/Comprar", payload=payload)
    def sell(self, payload: dict, especie_d: bool = False) -> Any: return self._request("POST", "/api/v2/operar/VenderEspecieD" if especie_d else "/api/v2/operar/Vender", payload=payload)
    def fci_subscribe(self, payload: dict) -> Any: return self._request("POST", "/api/v2/operar/suscripcion/fci", payload=payload)
    def fci_redeem(self, payload: dict) -> Any: return self._request("POST", "/api/v2/operar/rescate/fci", payload=payload)
    def get_movements(self, date_from: str, date_to: str, country: str, currency: Optional[str] = None) -> Any:
        payload = {"fechaDesde": date_from, "fechaHasta": date_to, "pais": country}
        if currency: payload["moneda"] = currency
        return self._request("POST", "/api/v2/Asesor/Movimientos", payload=payload)
    def raw_request(self, method: str, path: str, payload: Any = None) -> Any: return self._request(method, path, payload=payload)
