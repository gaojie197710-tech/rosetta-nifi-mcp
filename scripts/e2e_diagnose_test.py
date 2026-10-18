"""diagnose end-to-end test: builds a sandbox with an invalid processor and a back-pressured queue, checks
that diagnose reports both for a processor and for the group.
Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
REPLACE_TEXT = "org.apache.nifi.processors.standard.ReplaceText"


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool}")
            return json.loads(text)

        gid = (await call("create_process_group", name="mcp-e2e-diagnose", x=0, y=-900))["id"]
        try:
            ua = await call("create_processor", group_id=gid, processor_type=UPDATE_ATTRIBUTE, name="tag",
                            properties={"seen": "yes"})
            slow = await call("create_processor", group_id=gid, processor_type=REPLACE_TEXT, name="slow",
                              auto_terminate=["success", "failure"], below_id=ua["id"])
            await call("create_processor", group_id=gid, processor_type=REPLACE_TEXT, name="broken")
            q = await call("connect", source_id=ua["id"], destination_id=slow["id"], relationships=["success"],
                           back_pressure_count=1)
            await call("set_run_state", component_id=ua["id"], state="RUNNING")
            await call("inject_test_flowfile", destination_id=ua["id"], content="hello diagnose", count=2)
            for _ in range(20):
                d = await call("diagnose", component_id=slow["id"])
                if d.get("incoming") and d["incoming"][0].get("back_pressure") == "ACTIVE":
                    break
                await asyncio.sleep(0.5)
            assert d["incoming"][0]["back_pressure"] == "ACTIVE", d
            assert d["upstream"] == ["tag"], d

            d = await call("diagnose", component_id=ua["id"])
            assert d["state"] == "RUNNING" and d["downstream"] == ["slow"], d
            assert any("hello diagnose" in (e.get("content_preview") or "") for e in d.get("provenance", [])), d

            g = await call("diagnose", component_id=gid)
            names = {p["name"] for p in g["problem_components"]}
            assert {"broken", "slow"} <= names and "tag" not in names, names
            broken = next(p for p in g["problem_components"] if p["name"] == "broken")
            assert broken["first_error"] and "invalid" in broken["problems"], broken
            assert g["back_pressured_queues"][0]["id"] == q["id"], g
            assert len(json.dumps(g)) < 20000, "group diagnosis is not compact"

            print("\nPASS: diagnose reports invalid processors, back pressure, neighbours and provenance")
            return 0
        finally:
            await call("set_run_state", component_id=ua["id"], state="STOPPED")
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
