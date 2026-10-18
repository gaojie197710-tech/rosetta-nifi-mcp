"""Phase 3 end-to-end test: version control, export/import and auto-layout on a throwaway group,
using a temporary bucket in the NiFi Registry at NIFI_REGISTRY_URL (default http://localhost:18080)."""

import asyncio
import json
import os
import sys
import tempfile
import urllib.request

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

REGISTRY = os.environ.get("NIFI_REGISTRY_URL", "http://localhost:18080") + "/nifi-registry-api"


def registry(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(REGISTRY + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read() or "{}")


async def main() -> int:
    bucket = registry("POST", "/buckets", {"name": "mcp-e2e-test"})
    created_groups: list[str] = []
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool}")
            return json.loads(text)

        try:
            regs = (await call("list_registry_flows"))["registries"]
            reg_id = regs[0]["id"]
            buckets = (await call("list_registry_flows", registry_id=reg_id))["buckets"]
            assert any(b["id"] == bucket["identifier"] for b in buckets)

            # A and B are deliberately stacked on one spot so layout has an overlap to fix;
            # the funnel uses below_id placement.
            g = await call("create_process_group", name="mcp-e2e-registry")
            gid = g["id"]; created_groups.append(gid)
            a = await call("create_processor", group_id=gid, processor_type="org.apache.nifi.processors.attributes.UpdateAttribute", name="step A", x=0, y=0)
            b = await call("create_processor", group_id=gid, processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
                           name="step B", x=0, y=0)
            sink = await call("create_funnel", group_id=gid, below_id=b["id"])
            await call("connect", source_id=a["id"], destination_id=b["id"], relationships=["success"])
            await call("connect", source_id=b["id"], destination_id=sink["id"], relationships=["success"])
            await call("connect", source_id=a["id"], destination_id=sink["id"], relationships=["success"])  # skips a layer
            flow = await call("get_flow", process_group_id=gid)
            assert len({(p["id"]) for p in flow["processors"]}) == 2
            plan = await call("layout_process_group", group_id=gid, dry_run=True)
            laid = await call("layout_process_group", group_id=gid)
            assert plan["would_move"] > 0 and laid["moved"] > 0, (plan, laid)
            again = await call("layout_process_group", group_id=gid, dry_run=True)
            assert again["would_move"] == 0, again

            # Version control: start, modify, inspect, commit v2, roll back, revert.
            v1 = await call("commit_version", group_id=gid, comment="v1 from e2e", registry_id=reg_id, bucket_id=bucket["identifier"])
            assert v1["version"] == "1" and v1["state"] == "UP_TO_DATE", v1
            await call("update_processor", processor_id=b["id"], properties={"stage": "two"})
            st = await call("get_version_status", group_id=gid)
            assert st["state"] == "LOCALLY_MODIFIED" and st["local_changes"], st
            v2 = await call("commit_version", group_id=gid, comment="v2 adds stage attribute")
            assert v2["version"] == "2", v2
            back = await call("change_version", group_id=gid, version="1")
            assert back["version"] == "1" and back["state"] == "STALE", back
            await call("update_processor", processor_id=a["id"], name="renamed locally")
            reverted = await call("revert_local_changes", group_id=gid)
            assert reverted["state"] == "STALE", reverted
            latest = await call("change_version", group_id=gid)
            assert latest["version"] == "2" and latest["state"] == "UP_TO_DATE", latest

            flows = (await call("list_registry_flows", registry_id=reg_id, bucket_id=bucket["identifier"]))["flows"]
            assert len(flows[0]["versions"]) == 2

            # Promotion path: instantiate the registry flow elsewhere; and a JSON export/import round trip.
            promoted = await call("import_from_registry", parent_id="root", registry_id=reg_id,
                                  bucket_id=bucket["identifier"], flow_id=flows[0]["flow_id"])
            created_groups.append(promoted["id"])
            assert promoted["version"] == "2", promoted
            with tempfile.TemporaryDirectory() as tmp:
                os.environ["NIFI_MCP_EXPORT_DIR"] = tmp
                exported = await call("export_flow", group_id=gid, file_name="flow.json")
                for bad in ("../escape.json", "/etc/x.json", "flow.json"):
                    r = await client.call_tool("export_flow", {"group_id": gid, "file_name": bad})
                    assert r.is_error, f"export_flow accepted {bad!r}"
                assert exported["processors"] == 2, exported
                copy = await call("import_flow_definition", parent_id="root", file_path=exported["file"], group_name="mcp-e2e-imported")
                created_groups.append(copy["id"])
            copied = await call("get_flow", process_group_id=copy["id"])
            assert len(copied["processors"]) == 2 and len(copied["connections"]) == 3, copied

            print(f"\nPASS: layout moved {laid['moved']} (planned {plan['would_move']}), versions 1-2 committed, "
                  "rolled back, reverted, upgraded, promoted and round-tripped")
            return 0
        finally:
            for g in reversed(created_groups):
                await call("delete_component", component_id=g, kind="process_group")
            for f in registry("GET", f"/buckets/{bucket['identifier']}/flows"):
                registry("DELETE", f"/buckets/{bucket['identifier']}/flows/{f['identifier']}?version=0")
            registry("DELETE", f"/buckets/{bucket['identifier']}?version={registry('GET', '/buckets/' + bucket['identifier'])['revision']['version']}")


sys.exit(asyncio.run(main()))
