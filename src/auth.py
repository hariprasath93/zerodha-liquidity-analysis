"""
Zerodha automated authentication module.

Handles daily login using stored credentials and TOTP-based two-factor
authentication. Generates the access token for the trading session.
"""

import json
import logging
import re
import time
from typing import Optional
from urllib.parse import urlparse, parse_qs

import pyotp
import requests
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)

# Zerodha Kite endpoints (use kite.zerodha.com so session cookies are sent)
KITE_LOGIN_URL = "https://kite.zerodha.com/api/login"
KITE_TWOFA_URL = "https://kite.zerodha.com/api/twofa"
KITE_CONNECT_URL = "https://kite.zerodha.com/connect/login"

# Browser-like headers so /connect/authorize may redirect to redirect_url with token
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Upgrade-Insecure-Requests": "1",
}


class ZerodhaAuth:
    """Manages Zerodha authentication lifecycle."""

    def __init__(self, config: dict):
        """
        Initialize with credentials from config.

        Args:
            config: Dict containing zerodha credentials section.
        """
        self.api_key = config["api_key"]
        self.api_secret = config["api_secret"]
        self.user_id = config["user_id"]
        self.password = config["password"]
        self.totp_secret = config["totp_secret"]

        self.kite = KiteConnect(api_key=self.api_key)
        self.access_token = None
        self._session = requests.Session()

    def login(self) -> KiteConnect:
        """
        Perform full login flow:
          1. POST credentials to get request_id
          2. Generate TOTP and submit 2FA
          3. Fetch request_token via redirect (or from response body)
          4. Exchange for access_token

        Returns:
            Authenticated KiteConnect instance.

        Raises:
            AuthenticationError: If any step of the login fails.
        """
        logger.info("Starting Zerodha login for user %s", self.user_id)
        logger.debug("Login endpoints: login=%s twofa=%s connect=%s", KITE_LOGIN_URL, KITE_TWOFA_URL, KITE_CONNECT_URL)

        try:
            logger.debug("Step 1: Submitting credentials")
            request_id = self._submit_credentials()
            logger.debug("Step 1 done: request_id=%s", request_id)

            logger.debug("Step 2: Submitting 2FA (TOTP)")
            self._submit_twofa(request_id)
            logger.debug("Step 2 done: 2FA accepted")

            logger.debug("Step 3: Fetching request_token via connect URL")
            request_token = self._get_request_token()
            logger.debug("Step 3 done: request_token length=%d", len(request_token))

            logger.debug("Step 4: Exchanging request_token for access_token")
            self._generate_session(request_token)

            logger.info("Login successful. Access token obtained.")
            return self.kite

        except Exception as e:
            logger.error("Login failed: %s", str(e))
            raise AuthenticationError(f"Login failed: {e}") from e

    def _submit_credentials(self) -> str:
        """Submit user_id and password, return request_id."""
        logger.debug("POST %s (user_id=%s)", KITE_LOGIN_URL, self.user_id)
        resp = self._session.post(
            KITE_LOGIN_URL,
            data={
                "user_id": self.user_id,
                "password": self.password,
            },
        )
        logger.debug("Login response status=%s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
        logger.debug("Login response data keys=%s status=%s", list(data.keys()), data.get("status"))

        if data.get("status") != "success":
            raise AuthenticationError(
                f"Credential submission failed: {data.get('message', 'Unknown error')}"
            )

        request_id = data["data"]["request_id"]
        logger.debug("Got request_id: %s", request_id)
        return request_id

    def _submit_twofa(self, request_id: str) -> None:
        """Generate TOTP and submit two-factor authentication."""
        totp = pyotp.TOTP(self.totp_secret)
        twofa_value = totp.now()
        logger.debug("Generated TOTP for 2FA (request_id=%s), value length=%d", request_id, len(str(twofa_value)))

        logger.debug("POST %s with request_id and twofa_value", KITE_TWOFA_URL)
        resp = self._session.post(
            KITE_TWOFA_URL,
            data={
                "user_id": self.user_id,
                "request_id": request_id,
                "twofa_value": twofa_value,
                "twofa_type": "totp",
            },
        )
        logger.debug("2FA response status=%s", resp.status_code)
        resp.raise_for_status()
        data = resp.json()
        logger.debug("2FA response status=%s", data.get("status"))

        if data.get("status") != "success":
            logger.warning("First TOTP attempt failed, retrying after rotation...")
            logger.debug("2FA failure message: %s", data.get("message", ""))
            time.sleep(max(1, 30 - (int(time.time()) % 30) + 1))
            twofa_value = totp.now()

            resp = self._session.post(
                KITE_TWOFA_URL,
                data={
                    "user_id": self.user_id,
                    "request_id": request_id,
                    "twofa_value": twofa_value,
                    "twofa_type": "totp",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                raise AuthenticationError(
                    f"2FA failed: {data.get('message', 'Unknown error')}"
                )

        logger.debug("2FA submitted successfully")

    def _get_request_token(self) -> str:
        """
        Navigate to the Kite Connect login URL and follow the redirect chain
        until we get request_token (in Location or in body).
        """
        url = f"{KITE_CONNECT_URL}?v=3&api_key={self.api_key}"
        logger.debug("GET connect URL (same session for cookies): %s", url)

        old_headers = dict(self._session.headers)
        try:
            self._session.headers.update(BROWSER_HEADERS)
            resp = self._session.get(url, allow_redirects=False)
        finally:
            self._session.headers.clear()
            self._session.headers.update(old_headers)
        redirect_url = resp.headers.get("Location", "")
        logger.debug("Connect response status=%s Location=%s", resp.status_code, redirect_url[:100] if redirect_url else "")

        max_redirects = 10
        for _ in range(max_redirects):
            if redirect_url and "request_token" in redirect_url:
                parsed = urlparse(redirect_url)
                params = parse_qs(parsed.query)
                request_token = params["request_token"][0]
                logger.debug("Got request_token from redirect (host=%s)", parsed.netloc)
                return request_token

            if not redirect_url:
                request_token = self._extract_request_token_from_response(resp)
                if request_token:
                    logger.debug("Got request_token from response body")
                    return request_token
                raise AuthenticationError("Redirect chain ended with no Location header and no request_token in body")

            logger.debug("Following redirect to: %s", redirect_url[:100])
            self._session.headers.update(BROWSER_HEADERS)
            self._session.headers["Referer"] = "https://kite.zerodha.com/"
            try:
                resp = self._session.get(redirect_url, allow_redirects=False)
            finally:
                for k in BROWSER_HEADERS:
                    self._session.headers.pop(k, None)
                self._session.headers.pop("Referer", None)
            redirect_url = resp.headers.get("Location", "")
            logger.debug("Next response status=%s Location=%s", resp.status_code, redirect_url[:100] if redirect_url else "")

        raise AuthenticationError("Too many redirects; could not find request_token")

    def _extract_request_token_from_response(self, resp) -> Optional[str]:
        """Try to extract request_token from a 200 response body (JSON or HTML)."""
        if resp.status_code != 200:
            return None
        text = resp.text or ""
        if not text.strip():
            return None
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "request_token" in data:
                return data["request_token"]
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
                if "request_token" in data["data"]:
                    return data["data"]["request_token"]
        except (json.JSONDecodeError, TypeError):
            pass
        patterns = [
            r"request_token=([A-Za-z0-9_.-]+)",
            r'"request_token"\s*:\s*"([^"]+)"',
            r"'request_token'\s*:\s*'([^']+)'",
            r"request_token[\"']?\s*[:=]\s*[\"']([^\"']+)[\"']",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                token = match.group(1).strip()
                if len(token) >= 10:
                    return token
        logger.debug("Response body (first 500 chars): %s", text[:500])
        return None

    def _generate_session(self, request_token: str) -> None:
        """Exchange request_token for access_token."""
        logger.debug("Calling kite.generate_session with request_token length=%d", len(request_token))
        data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        self.access_token = data["access_token"]
        self.kite.set_access_token(self.access_token)
        logger.debug("Session generated; access_token length=%d", len(self.access_token))
        logger.info("Session generated. Access token set on KiteConnect instance.")

    def get_kite(self) -> KiteConnect:
        """Return the authenticated KiteConnect instance."""
        if self.access_token is None:
            raise AuthenticationError("Not logged in. Call login() first.")
        return self.kite

    def get_access_token(self) -> str:
        """Return the current access token."""
        if self.access_token is None:
            raise AuthenticationError("Not logged in. Call login() first.")
        return self.access_token


class AuthenticationError(Exception):
    """Raised when Zerodha authentication fails."""
    pass
