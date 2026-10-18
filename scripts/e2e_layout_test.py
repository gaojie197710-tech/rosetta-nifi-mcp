"""Layout end-to-end test: build_flow lays out a fan-out pipeline with nothing colliding on the canvas.

A ListenHTTP processor feeds two http_stage groups (Enrich -> Price); each stage's rejected, failure and ledger
ports go to funnels, either shared by both stages or one funnel per stage, under the "stack" and the "auto"
layout. For every canvas built (the top group and each stage group) the component positions, connection
bends and label boxes read back from the NiFi API must not overlap, and no line may run through a component
it does not connect (canvas.clutter). Run with NIFI_MCP_ALLOW_WRITES=true; E2E_KEEP=1 keeps the groups.
"""

import asyncio
import json
import os
import sys

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.canvas import clutter  # noqa: E402
from rosetta_nifi_mcp.client import NiFiClient  # noqa: E402
from rosetta_nifi_mcp.server import mcp  # noqa: E402

LISTEN = "org.apache.nifi.processors.standard.ListenHTTP"
STAGES = ("10 · Enrich", "20 · Price")
SIDE = ("rejected", "failure", "ledger")


def spec(layout: str, shared: bool) -> dict:
    sink = (lambda g, p: p) if shared else (lambda g, p: f"{p} {g[:2]}")
    connections = [{"source": "Receive", "destination": "10 · Enrich/in", "relationships": ["success"]},
                   {"source": "10 · Enrich/success", "destination": "20 · Price/in"},
                   {"source": "20 · Price/success", "destination": "done"}]
    connections += [{"source": f"{g}/{p}", "destination": sink(g, p)} for g in STAGES for p in SIDE]
    return {
        "name": f"mcp-e2e-layout-{layout}-{'shared' if shared else 'per-stage'}", "layout": layout,
        "processors": [{"name": "Receive", "type": LISTEN, "properties": {"Listening Port": "18555"}}],
        "funnels": sorted({sink(g, p) for g in STAGES for p in SIDE}) + ["done"],
        "groups": [{"name": g, "stage": {"stage": g[5:].lower(), "url": f"http://127.0.0.1:1/{g[5:].lower()}",
                                         "prefix": "t"}} for g in STAGES],
        "connections": connections}


async def main() -> int:
    nifi = NiFiClient()
    built: list[str] = []
    failures: list[str] = []
    async with Client(mcp) as client:
        try:
            for layout in ("stack", "auto"):
                for shared in (True, False):
                    r = await client.call_tool("build_flow", spec(layout, shared))
                    text = "".join(getattr(c, "text", "") for c in r.content)
                    if r.is_error:
                        raise RuntimeError(f"build_flow failed: {text[:1500]}")
                    flow = json.loads(text)
                    built.append(flow["group_id"])
                    canvases = {"top": flow["group_id"]}
                    canvases |= {name: g["group_id"] for name, g in flow["groups"].items()}
                    for name, gid in canvases.items():
                        f = (await nifi.get(f"/flow/process-groups/{gid}"))["processGroupFlow"]
                        problems = clutter(f["flow"])
                        label = f"{layout} {'shared' if shared else 'per-stage'}: {name}"
                        print(f"{'ok ' if not problems else 'BAD'} {label}")
                        failures += [f"{label}: {p}" for p in problems]
        finally:
            if os.environ.get("E2E_KEEP"):
                print(f"    kept groups {built}")
            else:
                for gid in built:
                    await client.call_tool("delete_component", {"component_id": gid, "kind": "process_group"})
            await nifi.aclose()
    if failures:
        print("\nFAIL:\n  " + "\n  ".join(failures))
        return 1
    print("\nPASS: no overlapping boxes or labels, no line through a component, on every canvas")
    return 0


sys.exit(asyncio.run(main()))
