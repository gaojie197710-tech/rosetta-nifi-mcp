"""Phase 2 end-to-end test: builds a small flow in a throwaway group, pushes test data through it,
checks the output, then deletes the group. Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool}")
            return json.loads(text)

        group = await call("create_process_group", name="mcp-e2e-sandbox", x=0, y=-600)
        gid = group["id"]
        try:
            ua = await call("create_processor", group_id=gid, processor_type="org.apache.nifi.processors.attributes.UpdateAttribute",
                            name="tag order", properties={"order.checked": "${order.id:isEmpty():not()}"}, x=500)
            sink = await call("create_funnel", group_id=gid, x=500, y=300)
            out = await call("connect", source_id=ua["id"], destination_id=sink["id"], relationships=["success"])

            await call("set_run_state", component_id=ua["id"], state="RUNNING")
            sent = await call("inject_test_flowfile", destination_id=ua["id"], content='{"order": 42}', attributes={"order.id": "42"})
            assert sent["sent"] == 1, sent

            for _ in range(20):
                q = await call("list_queue", connection_id=out["id"])
                if q["flowfiles"]:
                    break
                await asyncio.sleep(0.5)
            assert q["flowfiles"], "nothing reached the output queue"
            ff = await call("get_flowfile", connection_id=out["id"], flowfile_uuid=q["flowfiles"][0]["uuid"])
            assert ff["attributes"]["order.checked"] == "true", ff["attributes"]
            assert ff["content"] == '{"order": 42}', ff["content"]

            # Reuse path: second injection updates the existing injector and drops the old attribute.
            await call("inject_test_flowfile", destination_id=ua["id"], content="second", attributes={"other": "x"})
            await call("set_run_state", component_id=ua["id"], state="STOPPED")
            await call("update_processor", processor_id=ua["id"], properties={"order.checked": "updated"})
            await call("get_bulletins", group_id=gid)
            print("\nPASS: flow built, test data injected, output verified")
            return 0
        finally:
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
