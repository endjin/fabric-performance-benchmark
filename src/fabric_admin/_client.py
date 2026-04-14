import json
import logging
import time

import requests
from azure.identity import InteractiveBrowserCredential

logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com"
FABRIC_SCOPE = f"{FABRIC_API_BASE}/.default"


class FabricRestClient:
    """Low-level client for the Microsoft Fabric REST API.

    Handles authentication (via a supplied token or interactive browser login)
    and provides helpers for common HTTP patterns including long-running
    operation (LRO) polling.
    """

    def __init__(self, token: str | None = None):
        if token:
            self._token = token
        else:
            logger.info("No token supplied — acquiring token via interactive browser login")
            credential = InteractiveBrowserCredential()
            self._token = credential.get_token(FABRIC_SCOPE).token
            logger.info("Token acquired successfully")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def get(self, url: str, **kwargs) -> requests.Response:
        logger.debug("GET %s", url)
        response = requests.get(url, headers=self.headers, **kwargs)
        logger.debug("GET %s → %s", url, response.status_code)
        return response

    def post(self, url: str, json_body: dict | None = None, **kwargs) -> requests.Response:
        logger.debug("POST %s", url)
        response = requests.post(url, headers=self.headers, json=json_body, **kwargs)
        logger.debug("POST %s → %s", url, response.status_code)
        return response

    def poll_long_running_operation(
        self,
        location_url: str,
        max_retries: int = 10,
        retry_interval: int = 2,
    ) -> requests.Response:
        """Poll a Fabric LRO until it reaches a terminal state.

        Returns the final poll response whose ``status`` is ``Succeeded``.
        """
        for attempt in range(1, max_retries + 1):
            logger.debug("LRO poll attempt %d/%d: GET %s", attempt, max_retries, location_url)
            response = self.get(location_url)
            body = response.json()

            status = body.get("status")
            logger.debug("LRO status: %s", status)

            if status == "Succeeded":
                return response
            if status in ("Running", "NotStarted", "Undefined"):
                wait = int(response.headers.get("Retry-After", retry_interval))
                logger.debug("LRO in progress — waiting %ds", wait)
                time.sleep(wait)
            elif status == "Failed":
                error = body.get("error", {})
                raise RuntimeError(f"Long-running operation failed: {json.dumps(error, indent=2)}")
            else:
                raise RuntimeError(
                    f"Unexpected LRO status '{status}': {json.dumps(body, indent=2)}"
                )

        raise TimeoutError(f"Long-running operation did not complete after {max_retries} retries")

    def handle_lro_response(self, response: requests.Response) -> requests.Response:
        """If *response* is a 202, poll the LRO to completion and return the
        final result response.  If 200, return as-is."""
        if response.status_code == 200:
            return response

        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError("202 response missing Location header for LRO polling")
            logger.debug("202 Accepted — polling LRO at %s", location)
            operation_response = self.poll_long_running_operation(location)

            result_location = operation_response.headers.get("Location")
            if result_location:
                logger.debug("Fetching LRO result from %s", result_location)
                return self.get(result_location)
            return operation_response

        response.raise_for_status()
        return response  # unreachable, but keeps type checkers happy
