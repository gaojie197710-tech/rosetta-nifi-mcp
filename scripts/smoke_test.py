"""Calls every read-only tool once against the live NiFi through an in-memory MCP client."""

import asyncio
import json

from mcp import Client

from rosetta_nifi_mcp.server import mcp


async def main() -> None:
    async with Client(mcp) as client:
        async def call(name: str, **args):
            result = await client.call_tool(name, args)
            text = "".join(getattr(c, "text", "") for c in result.content)
            if result.is_error:
                print(f"--- {name} {args} [ERROR]\n{text[:600]}\n")
                return None
            data = json.loads(text)
            print(f"--- {name} {args} [ok]\n{json.dumps(data, ensure_ascii=False)[:500]}\n")
            return data

        await call("get_about")
        root = await call("get_flow")
        await call("search_processor_types", query="json", limit=3)
        await call("get_processor_schema", processor_type="org.apache.nifi.processors.standard.GenerateFlowFile")
        await call("get_bulletins", limit=5)
        await call("search_flow", query="DEMO")
        # Walk every group to exercise the processor, provenance, queue and FlowFile tools.
        stack, proc, conn = ["root"], None, None
        while stack and not (proc and conn):
            flow = json.loads((await client.call_tool("get_flow", {"process_group_id": stack.pop()})).content[0].text)
            stack += [g["id"] for g in flow["process_groups"]]
            proc = proc or (flow["processors"] or [None])[0]
            conn = conn or next((c for c in flow["connections"] if c["queued"] and not c["queued"].startswith("0 ")), None)
        if proc:
            await call("get_processor", processor_id=proc["id"])
            await call("search_provenance", component_id=proc["id"], max_results=3)
        if conn:
            q = await call("list_queue", connection_id=conn["id"])
            if q and q["flowfiles"]:
                await call("get_flowfile", connection_id=conn["id"], flowfile_uuid=q["flowfiles"][0]["uuid"])
        else:
            print("(no non-empty queue found; list_queue/get_flowfile not exercised)")


asyncio.run(main())
