from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from iol_cli.config import Config, load_config
from iol_cli.iol_client import IOLAPIError, IOLClient


class ReadOnlyIOLGateway:
    """A deliberately narrow IOL facade: it exposes no mutation method."""

    def __init__(self, client: IOLClient):
        self._client = client

    def account_status(self) -> Any:
        return self._client.get_account_status()

    def portfolio(self, country: str) -> Any:
        return self._client.get_portfolio(country)

    def orders(self) -> Any:
        return self._client.list_orders()

    def movements(self, date_from: date, date_to: date, country: str = "argentina") -> Any:
        """Return movements through IOL's read-only data endpoint.

        IOL models this query as a POST request, but the gateway never exposes
        arbitrary requests or business mutations to callers.
        """
        return self._client.get_movements(date_from.isoformat(), date_to.isoformat(), country)

    def optional_movements(self, as_of: date) -> tuple[Any | None, str | None]:
        try:
            return self.movements(as_of - timedelta(days=90), as_of), None
        except IOLAPIError as exc:
            # The IOL movement endpoint is not consistently enabled for all accounts.
            return None, str(exc)


def configured_read_only_gateway(config: Config | None = None) -> ReadOnlyIOLGateway:
    """Build the narrow IOL facade without exposing the operational client to callers."""
    config = config or load_config()
    return ReadOnlyIOLGateway(IOLClient(config.username, config.password, config.base_url, config.timeout))
