"""health_check end-to-end: adds a sandbox with two stopped listeners on one port and one on the NiFi Registry's
port 18080 (stopped, so nothing binds), an invalid processor and a back-pressured queue, and checks every one is
reported along with registry reachability, version control and system diagnostics. Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

LISTEN = "org.apache.nifi.processors.standard.ListenHTTP"
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
GROUP = "mcp-e2e-health"
SSL_SERVICE = "org.apache.nifi.ssl.StandardSSLContextService"
CERT_DIR = Path(tempfile.mkdtemp())


def make_cert(name: str, days: int) -> Path:
    """A throwaway self-signed PEM certificate expiring in `days` days (negative: already expired)."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = dt.datetime.now(dt.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(days=400))
            .not_valid_after(now + dt.timedelta(days=days, hours=1)).sign(key, hashes.SHA256()))
    path = CERT_DIR / f"{name}.pem"
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return path


os.environ["NIFI_MCP_EXTRA_CERTS"] = os.pathsep.join(
    [str(make_cert("mcp-critical", 3)), str(make_cert("mcp-warning", 20)), str(make_cert("mcp-expired", -2)),
     str(make_cert("mcp-fine", 400)), str(CERT_DIR / "secret-store.p12")])


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool}")
            return json.loads(text)

        gid = (await call("create_process_group", name=GROUP, x=0, y=-1200))["id"]
        try:
            for name, port in (("listener a", "18097"), ("listener b", "18097"), ("on registry port", "18080")):
                await call("create_processor", group_id=gid, processor_type=LISTEN, name=name,
                           properties={"Listening Port": port}, auto_terminate=["success"])
            ua = await call("create_processor", group_id=gid, processor_type=UPDATE_ATTRIBUTE, name="feeder")
            blocked = await call("create_processor", group_id=gid, processor_type=UPDATE_ATTRIBUTE, name="blocked",
                                 auto_terminate=["success"])
            await call("connect", source_id=ua["id"], destination_id=blocked["id"], relationships=["success"], back_pressure_count=1)
            await call("create_processor", group_id=gid, processor_type=UPDATE_ATTRIBUTE, name="dangling")
            await call("create_controller_service", group_id=gid, service_type=SSL_SERVICE, name="mcp p12 ssl",
                       properties={"Keystore Filename": "/tmp/mcp-no-such.p12", "Keystore Type": "PKCS12"})
            await call("set_run_state", component_id=ua["id"], state="RUNNING")
            await call("inject_test_flowfile", destination_id=ua["id"], content="x", count=2)
            await asyncio.sleep(2)

            h = await call("health_check")
            print("    issues:", json.dumps(h["issues"])[:600])
            assert not h["healthy"]
            mine = next(g for g in h["groups"] if g["group"] == f"/{GROUP}")
            assert "dangling" in mine["invalid"], mine
            assert any(q["group"] == f"/{GROUP}" and q["back_pressure"] == "ACTIVE" for q in h["back_pressured_queues"]), h["back_pressured_queues"]
            reasons = {(c["port"], c["reason"].split(" ")[0]) for c in h["port_conflicts"]}
            assert ("18097", "used") in reasons and ("18080", "NiFi") in reasons, h["port_conflicts"]
            assert any("port 18080 conflict" in i for i in h["issues"]), h["issues"]
            assert all(r["reachable"] for r in h["registries"]) and h["registries"], h["registries"]
            assert h["system"]["heap_utilization"] and h["system"]["disk"], h["system"]
            assert all(v["state"] != "UP_TO_DATE" for v in h["version_control"]), h["version_control"]
            drift = [v for v in h["version_control"] if v["state"] != "UP_TO_DATE"]
            assert all(any(v["group"] in i for i in h["issues"]) for v in drift), (drift, h["issues"])
            assert len(json.dumps(h)) < 40000, "health_check output is not compact"

            certs = {c["subject"]: c for c in h["certificates"]["certificates"]}
            assert certs["CN=mcp-critical"]["status"] == "critical" and certs["CN=mcp-critical"]["days_left"] == 3, certs
            assert certs["CN=mcp-warning"]["status"] == "warning" and certs["CN=mcp-expired"]["status"] == "expired", certs
            assert certs["CN=mcp-fine"]["status"] == "ok" and certs["CN=nifi-mcp"]["status"] == "ok", certs
            server = next(c for c in h["certificates"]["certificates"] if c["source"].startswith("NiFi server"))
            assert server.get("days_left") is not None, server
            print(f"    NiFi server certificate: {server['status']}, {server['days_left']} days left")
            assert any("CRITICAL" in i and "mcp-critical" in i for i in h["issues"]), h["issues"]
            assert any("EXPIRED" in i and "mcp-expired" in i for i in h["issues"]), h["issues"]
            assert not any("mcp-fine" in i for i in h["issues"])
            skipped = h["certificates"]["skipped"]
            assert any(x["path"].endswith("secret-store.p12") and "password" in x["reason"] for x in skipped if "path" in x), skipped
            assert any("mcp p12 ssl" in x["source"] and "password" in x["reason"] for x in skipped), skipped
            assert "assword\": \"" not in json.dumps(h), "a password value leaked"
            print("\nPASS: health_check flags invalid components, back pressure, port conflicts and VC drift")
            return 0
        finally:
            await call("set_run_state", component_id=ua["id"], state="STOPPED")
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
