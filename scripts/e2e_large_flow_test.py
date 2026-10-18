"""Large-flow compactness test: generates a 400-processor sandbox group (35 chains of 10 UpdateAttribute steps,
whose heads are invalid for lack of an upstream connection, plus 50 invalid ReplaceText), then checks get_flow, diagnose, describe_flow and health_check stay compact
by default, and that status/name/type filters and paging reach every component.
Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys
import tempfile
import time

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")
os.environ["NIFI_MCP_EXPORT_DIR"] = tempfile.mkdtemp()

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
REPLACE_TEXT = "org.apache.nifi.processors.standard.ReplaceText"
MAX_CHARS = 12000  # about 3,000 tokens


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool} {json.dumps({k: v for k, v in args.items() if k != 'processors'})[:90]} -> {len(text)} chars")
            return json.loads(text), len(text)

        processors, connections = [], []
        for i in range(350):
            last = i % 10 == 9
            processors.append({"name": f"step {i:03d}", "type": UPDATE_ATTRIBUTE, "properties": {"n": str(i)},
                               **({"auto_terminate": ["success"]} if last else {})})
            if not last:
                connections.append({"source": f"step {i:03d}", "destination": f"step {i + 1:03d}", "relationships": ["success"]})
        processors += [{"name": f"broken {i:02d}", "type": REPLACE_TEXT} for i in range(50)]
        t = time.monotonic()
        flow, _ = await call("build_flow", name="mcp-e2e-large", processors=processors, connections=connections)
        print(f"    built 400 processors in {time.monotonic() - t:.0f}s")
        gid = flow["group_id"]
        try:
            f, n = await call("get_flow", process_group_id=gid)
            assert n < MAX_CHARS and f["counts"]["processors"] == 400 and f["counts"]["invalid"] == 85, f["counts"]
            assert len(f["processors"]) == 25 and f["paging"][0]["next_offset"] == 25, f["paging"]

            f, _ = await call("get_flow", process_group_id=gid, status="invalid", limit=100)
            assert len(f["processors"]) == 85 and sum(p["name"].startswith("broken") for p in f["processors"]) == 50
            seen, offset = set(), 0
            while offset is not None:
                f, n = await call("get_flow", process_group_id=gid, type="UpdateAttribute", offset=offset, limit=100)
                seen |= {p["id"] for p in f["processors"]}
                offset = next((p.get("next_offset") for p in f.get("paging", []) if p["what"] == "processors"), None)
            assert len(seen) == 350, len(seen)
            f, _ = await call("get_flow", process_group_id=gid, name="step 12")
            assert sorted(p["name"] for p in f["processors"]) == [f"step {i}" for i in range(120, 130)], f["processors"]

            d, n = await call("diagnose", component_id=gid)
            assert n < MAX_CHARS and d["problem_counts"]["invalid"] == 85 and len(d["problem_components"]) == 20, d.get("paging")
            d, n = await call("diagnose", component_id=gid, name="broken 4", detail=0)
            assert len(d["problem_components"]) == 10 and "details" not in d, d

            d, n = await call("describe_flow", group_id=gid, markdown_file="large.md")
            assert n < MAX_CHARS and d["step_counts"]["total"] == 400 and len(d["steps"]) == 25, d.get("paging")
            md = open(d["markdown_file"]).read()
            assert "**step 349**" in md and "**broken 49**" in md, "markdown is not complete"
            d, _ = await call("describe_flow", group_id=gid, type="ReplaceText", limit=100)
            assert len(d["steps"]) == 50, len(d["steps"])

            h, n = await call("health_check")
            assert n < MAX_CHARS * 1.5, n
            mine = next(g for g in h["groups"] if g["group"] == "/mcp-e2e-large")
            assert mine["invalid_total"] == 85 and len(mine["invalid"]) == 8, mine
            print("\nPASS: 400-processor group stays compact by default; filters and paging reach every component")
            return 0
        finally:
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
