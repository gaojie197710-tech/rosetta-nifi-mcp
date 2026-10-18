"""test_expression end-to-end: Expression Language, RecordPath and Jolt through NiFi itself, including
NiFi's error messages for a bad expression, and that the hidden sandbox group is gone afterwards.
Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.expression import SANDBOX_NAME  # noqa: E402
from rosetta_nifi_mcp.server import mcp  # noqa: E402

SAMPLE = json.dumps({"order": {"id": 42, "items": [{"sku": "A"}, {"sku": "B"}]}, "customer": "acme"})


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error:
                raise RuntimeError(f"{tool} failed: {text[:800]}")
            print(f"ok  {tool} {args.get('language', '')} {args.get('expression', '')[:40]}")
            return json.loads(text)

        r = await call("test_expression", expression="${name:toUpper()}-${qty:plus(1)}", attributes={"name": "nifi", "qty": "41"})
        assert r == {"language": "expression_language", "ok": True, "result": "NIFI-42"}, r

        r = await call("test_expression", expression="${name:toUpper(}", attributes={"name": "x"})
        assert not r["ok"] and r["error"], r
        print("    EL error:", str(r["error"])[:160])

        r = await call("test_expression", language="record_path", expression="/order/id", content=SAMPLE)
        assert r["ok"] and str(r["result"]) == "42", r
        r = await call("test_expression", language="record_path", expression="count(/order/items[*])", content=SAMPLE)
        assert r["ok"] and str(r["result"]) == "2", r

        spec = json.dumps([{"operation": "shift", "spec": {"order": {"id": "orderId"}, "customer": "who"}}])
        r = await call("test_expression", language="jolt", expression=spec, content=SAMPLE)
        assert r["ok"] and r["result"] == {"orderId": 42, "who": "acme"}, r

        r = await call("test_expression", language="jolt", expression='[{"operation": "nosuch", "spec": {}}]', content=SAMPLE)
        assert not r["ok"] and r["error"], r
        print("    Jolt error:", str(r["error"])[:160])

        groups = (await call("get_flow"))["process_groups"]
        assert not any(g["name"] == SANDBOX_NAME for g in groups), "sandbox group left behind"
        print("\nPASS: EL, RecordPath and Jolt evaluated by NiFi, errors surfaced, sandbox removed")
        return 0


sys.exit(asyncio.run(main()))
