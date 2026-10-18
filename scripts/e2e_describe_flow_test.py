"""describe_flow end-to-end: builds a listener -> transform -> file flow with a parameter context, checks the
description (inputs with port/path, steps in order, outputs, endpoints, parameter context), writes it as
Markdown to a temporary export directory. Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")
EXPORT_DIR = tempfile.mkdtemp()
os.environ["NIFI_MCP_EXPORT_DIR"] = EXPORT_DIR

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

STD = "org.apache.nifi.processors.standard."


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, expect_error: bool = False, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error != expect_error:
                raise RuntimeError(f"{tool} {'failed' if r.is_error else 'unexpectedly succeeded'}: {text[:800]}")
            print(f"ok  {tool}{' (rejected as expected)' if expect_error else ''}")
            return text if expect_error else json.loads(text)

        flow = await call("build_flow", name="mcp-e2e-describe", processors=[
            {"name": "receive orders", "type": STD + "ListenHTTP",
             "properties": {"Listening Port": "18095", "Base Path": "orders"}},
            {"name": "stamp", "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
             "properties": {"env": "#{env}"}},
            {"name": "save order", "type": STD + "PutFile", "auto_terminate": ["success", "failure"],
             "properties": {"Directory": "/tmp/mcp-describe-out"}},
        ], connections=[{"source": "receive orders", "destination": "stamp", "relationships": ["success"]},
                        {"source": "stamp", "destination": "save order", "relationships": ["success"]}],
            parameter_context={"name": "mcp-e2e-describe-params", "parameters": [{"name": "env", "value": "dev"}]})
        gid, cid = flow["group_id"], flow["parameter_context_id"]
        try:
            d = await call("describe_flow", group_id=gid, markdown_file="describe.md")
            assert d["parameter_context"] == "mcp-e2e-describe-params", d
            assert [s["name"] for s in d["steps"]] == ["receive orders", "stamp", "save order"], d["steps"]
            src = next(i for i in d["inputs"] if i["name"] == "receive orders")
            assert src["endpoints"] == {"Listening Port": "18095", "Base Path": "orders"}, src
            sink = next(o for o in d["outputs"] if o["name"] == "save order")
            assert sink["endpoints"]["Directory"] == "/tmp/mcp-describe-out", sink
            assert all(o["name"] != "stamp" for o in d["inputs"] + d["outputs"]), d
            assert d["steps"][1]["config"] == {"env": "#{env}"} and d["steps"][1]["routes"] == {"success": ["save order"]}, d["steps"][1]
            assert any("18095" in e for e in d["external_endpoints"]), d["external_endpoints"]

            md = Path(d["markdown_file"]).read_text()
            assert Path(d["markdown_file"]).parent == Path(EXPORT_DIR).resolve()
            assert md.startswith("---\ntitle: mcp-e2e-describe") and "## Steps" in md and "1. **receive orders**" in md, md
            await call("describe_flow", expect_error=True, group_id=gid, markdown_file="describe.md")  # no overwrite
            await call("describe_flow", expect_error=True, group_id=gid, markdown_file="../escape.md")

            print("\nPASS: describe_flow reflects the real configuration and writes Markdown safely")
            return 0
        finally:
            await call("delete_component", component_id=gid, kind="process_group")
            await call("delete_parameter_context", context_id=cid)


sys.exit(asyncio.run(main()))
