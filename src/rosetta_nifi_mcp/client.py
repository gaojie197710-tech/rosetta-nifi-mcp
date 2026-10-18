"""Thin async client for the NiFi 2.x REST API, authenticated with a client certificate (mTLS)."""

from __future__ import annotations

import asyncio
import os
import ssl
from pathlib import Path
from typing import Any

import httpx
from mcp.server.mcpserver.exceptions import ToolError

DEFAULT_DIR = Path.home() / ".nifi-mcp"


class NiFiError(ToolError):
    pass


def _ssl_context() -> ssl.SSLContext:
    ca = os.environ.get("NIFI_CA_CERT", str(DEFAULT_DIR / "nifi-server-chain.pem"))
    cert = os.environ.get("NIFI_CLIENT_CERT", str(DEFAULT_DIR / "client.crt"))
    key = os.environ.get("NIFI_CLIENT_KEY", str(DEFAULT_DIR / "client.key"))
    ctx = ssl.create_default_context(cafile=ca)
    # NiFi's generated server cert is self-signed and may lack strict CA extensions.
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    ctx.load_cert_chain(cert, key)
    return ctx


class NiFiClient:
    def __init__(self, base_url: str | None = None) -> None:
        base = (base_url or os.environ.get("NIFI_URL", "https://localhost:9443")).rstrip("/")
        self._http = httpx.AsyncClient(base_url=f"{base}/nifi-api", verify=_ssl_context(), timeout=30)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        resp = await self._http.request(method, path, **kwargs)
        if resp.is_error:
            raise NiFiError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
        return resp

    async def get(self, path: str, **params: Any) -> Any:
        return (await self.request("GET", path, params=params or None)).json()

    async def post(self, path: str, body: Any) -> Any:
        return (await self.request("POST", path, json=body)).json()

    async def delete(self, path: str) -> None:
        await self.request("DELETE", path)

    async def run_async_request(self, create_path: str, body: Any, key: str, timeout: float = 20) -> dict:
        """NiFi's create -> poll until finished -> delete pattern (queue listings, provenance)."""
        created = (await self.post(create_path, body))[key]
        uri_path = f"{create_path}/{created['id']}"
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            current = created
            while not current.get("finished"):
                if asyncio.get_running_loop().time() > deadline:
                    raise NiFiError(f"{create_path} request did not finish within {timeout}s")
                await asyncio.sleep(0.5)
                current = (await self.get(uri_path))[key]
            return current
        finally:
            try:
                await self.delete(uri_path)
            except NiFiError:
                pass
