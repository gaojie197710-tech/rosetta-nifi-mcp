"""Certificate expiry for health_check: NiFi's server certificate (from the TLS handshake), this server's client
certificate, the CA bundle it trusts and any extra PEM files in NIFI_MCP_EXTRA_CERTS. Stores referenced by
SSL context services are listed but never read (their paths are on the NiFi host). Keystores and truststores that need a password are skipped and reported as such;
no password is ever read or printed."""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import socket
import ssl
from pathlib import Path
from urllib.parse import urlparse

from cryptography import x509

from .client import DEFAULT_DIR, NiFiClient, NiFiError

WARN_DAYS = 30
CRITICAL_DAYS = 7
PASSWORD_TYPES = (".p12", ".pfx", ".jks", ".bcfks")
MAX_PEM_BYTES = 1024 * 1024


def _entry(source: str, cert: x509.Certificate, now: dt.datetime) -> dict:
    expires = cert.not_valid_after_utc
    days = (expires - now).days
    status = "expired" if expires < now else "critical" if days < CRITICAL_DAYS else "warning" if days < WARN_DAYS else "ok"
    return {"source": source, "subject": cert.subject.rfc4514_string(), "expires": expires.date().isoformat(),
            "days_left": days, "status": status}


def _pem_file(source: str, path: Path, now: dt.datetime) -> list[dict]:
    try:
        if path.stat().st_size > MAX_PEM_BYTES:
            raise ValueError(f"larger than {MAX_PEM_BYTES} bytes")
        certs = x509.load_pem_x509_certificates(path.read_bytes())
    except (OSError, ValueError) as e:
        return [{"source": source, "path": str(path), "status": "unreadable", "reason": str(e)[:200]}]
    return [{**_entry(source, c, now), "path": str(path)} for c in certs]


def _server_cert(url: str) -> bytes:
    u = urlparse(url)
    ctx = ssl.create_default_context()
    # Read the certificate even when it is expired or untrusted: that is exactly what we want to report.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((u.hostname, u.port or 443), timeout=10) as sock:
        with ctx.wrap_socket(sock, server_hostname=u.hostname) as tls:
            return tls.getpeercert(binary_form=True)


async def _ssl_services(nifi: NiFiClient) -> list[dict]:
    services = []
    try:
        services += (await nifi.get("/flow/controller/controller-services"))["controllerServices"]
    except NiFiError:
        pass
    services += (await nifi.get("/flow/process-groups/root/controller-services", includeAncestorGroups="false",
                                includeDescendantGroups="true"))["controllerServices"]
    return [s for s in services if "SSLContext" in s["component"]["type"]]


async def check_certificates(nifi: NiFiClient) -> dict:
    now = dt.datetime.now(dt.timezone.utc)
    certs: list[dict] = []
    skipped: list[dict] = []
    url = os.environ.get("NIFI_URL", "https://localhost:9443")
    if url.startswith("https"):
        try:
            der = await asyncio.to_thread(_server_cert, url)
            certs.append({**_entry("NiFi server (TLS handshake)", x509.load_der_x509_certificate(der), now),
                          "endpoint": urlparse(url).netloc})
        except OSError as e:
            certs.append({"source": "NiFi server (TLS handshake)", "status": "unreadable", "reason": str(e)[:200]})
    certs += _pem_file("nifi-mcp client certificate", Path(os.environ.get("NIFI_CLIENT_CERT", str(DEFAULT_DIR / "client.crt"))), now)
    certs += _pem_file("nifi-mcp trusted CA bundle", Path(os.environ.get("NIFI_CA_CERT", str(DEFAULT_DIR / "nifi-server-chain.pem"))), now)
    for extra in filter(None, os.environ.get("NIFI_MCP_EXTRA_CERTS", "").split(os.pathsep)):
        p = Path(extra).expanduser()
        if p.suffix.lower() in PASSWORD_TYPES:
            skipped.append({"source": "NIFI_MCP_EXTRA_CERTS", "path": str(p), "reason": "keystore format needs a password"})
        else:
            certs += _pem_file("extra certificate", p, now)

    for s in await _ssl_services(nifi):
        props = s["component"].get("properties") or {}
        name = s["component"]["name"]
        for kind in ("Keystore", "Truststore"):
            path = props.get(f"{kind} Filename")
            if not path:
                continue
            fmt = (props.get(f"{kind} Type") or "").upper()
            # Paths come from NiFi's configuration and live on the NiFi host: never resolved here.
            if fmt == "PEM" or path.lower().endswith((".pem", ".crt")):
                skipped.append({"source": f"SSL service '{name}'", "path": path,
                                "reason": f"PEM {kind.lower()} on the NiFi host; add it to NIFI_MCP_EXTRA_CERTS to check it"})
            else:
                skipped.append({"source": f"SSL service '{name}'", "path": path,
                                "reason": f"{kind.lower()} type {fmt or 'unknown'} needs a password; not read"})
    skipped.append({"source": "NiFi's own keystore and truststore (nifi.properties)",
                    "reason": "need passwords; the server certificate is covered by the TLS handshake instead"})
    return {"warn_days": WARN_DAYS, "critical_days": CRITICAL_DAYS,
            "certificates": sorted(certs, key=lambda c: c.get("days_left", -10**6)), "skipped": skipped}


def cert_issues(report: dict) -> list[str]:
    out = []
    for c in report["certificates"]:
        if c["status"] in ("expired", "critical", "warning"):
            out.append(f"certificate {c['status'].upper()}: {c['source']} ({c['subject']}) expires {c['expires']}, "
                       f"{c['days_left']} days left")
        elif c["status"] == "unreadable":
            out.append(f"certificate unreadable: {c['source']}: {c.get('reason')}")
    return out

