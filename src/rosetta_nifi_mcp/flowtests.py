"""Saved flow tests: cases under flow-tests/<group name>/ run through inject_test_flowfile and the output queue."""

from __future__ import annotations

import asyncio
import difflib
import json
import os
from pathlib import Path
from typing import Any

import yaml
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from .build import INJECTOR_PREFIX, WRITE, _drop_queue, _nifi, _processor_idle, _rev, _wait, inject
from .client import NiFiClient

REPO_TESTS = Path(__file__).resolve().parents[2] / "flow-tests"
CONTENT_MATCHES = ("exact", "contains", "json_subset")


def tests_dir() -> Path:
    return Path(os.environ.get("NIFI_MCP_FLOW_TESTS_DIR", str(REPO_TESTS))).expanduser()


def load_cases(group_name: str) -> list[dict]:
    """Every case in flow-tests/<group name>/ (*.yaml, *.yml, *.json; a file may hold one case or a list)."""
    base = tests_dir().resolve()
    d = (base / group_name).resolve()
    if d.parent != base:
        raise ToolError(f"Group name {group_name!r} cannot be used as a directory under {base}")
    if not d.is_dir():
        return []
    cases = []
    for f in sorted(d.iterdir()):
        if f.suffix not in (".yaml", ".yml", ".json") or f.is_symlink():
            continue
        data = json.loads(f.read_text()) if f.suffix == ".json" else yaml.safe_load(f.read_text())
        for i, c in enumerate(data if isinstance(data, list) else [data]):
            c.setdefault("name", f.stem if not isinstance(data, list) else f"{f.stem}[{i}]")
            c["_file"] = f.name
            cases.append(c)
    return cases


def _json_subset(expected: Any, actual: Any, path: str = "$") -> list[str]:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return [f"{path}: expected object, got {json.dumps(actual)[:100]}"]
        diffs = []
        for k, v in expected.items():
            if k not in actual:
                diffs.append(f"{path}.{k}: missing (expected {json.dumps(v)[:100]})")
            else:
                diffs += _json_subset(v, actual[k], f"{path}.{k}")
        return diffs
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) < len(expected):
            return [f"{path}: expected list of at least {len(expected)}, got {json.dumps(actual)[:100]}"]
        return [d for i, v in enumerate(expected) for d in _json_subset(v, actual[i], f"{path}[{i}]")]
    return [] if expected == actual else [f"{path}: expected {json.dumps(expected)}, got {json.dumps(actual)}"]


def check(expect: dict, attributes: dict, content: str) -> list[str]:
    """Differences between a case's expectations and one output FlowFile; empty means pass."""
    diffs = []
    for k, v in (expect.get("attributes") or {}).items():
        actual = attributes.get(k)
        if v is None and actual is not None:
            diffs.append(f"attribute {k}: expected absent, got {actual!r}")
        elif v is not None and actual != str(v):
            diffs.append(f"attribute {k}: expected {str(v)!r}, got {actual!r}")
    want = expect.get("content")
    if want is None:
        return diffs
    if not isinstance(want, dict) or len(want) != 1 or next(iter(want)) not in CONTENT_MATCHES:
        raise ToolError(f"expect.content must be one of {{exact|contains|json_subset: ...}}, got {want!r}")
    mode, value = next(iter(want.items()))
    if mode == "exact" and content != value:
        diffs.append("content differs:\n" + "\n".join(difflib.unified_diff(
            str(value).splitlines(), content.splitlines(), "expected", "actual", lineterm="")))
    elif mode == "contains" and str(value) not in content:
        diffs.append(f"content does not contain {value!r}; got {content[:300]!r}")
    elif mode == "json_subset":
        try:
            diffs += _json_subset(value, json.loads(content))
        except json.JSONDecodeError as e:
            diffs.append(f"content is not JSON ({e}): {content[:300]!r}")
    return diffs


async def _components(nifi: NiFiClient, group_id: str) -> tuple[dict, list[dict]]:
    flow = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]["flow"]
    named: dict[str, str] = {}
    for kind in ("processors", "inputPorts", "outputPorts"):
        for c in flow[kind]:
            named[c["component"]["name"]] = c["id"]
            named[c["id"]] = c["id"]
    for f in flow["funnels"]:
        named[f["id"]] = f["id"]
    for c in flow["connections"]:
        if c["component"].get("name"):
            named.setdefault(c["component"]["name"], c["id"])
    return named, flow["connections"]


def _output_connection(spec: str | dict, named: dict, connections: list[dict]) -> str:
    """The output is a connection (id or name), a component (name or id) whose single incoming queue is read,
    or {from: component, relationship: name} for the queue leaving a processor."""
    by_id = {c["id"]: c for c in connections}
    if isinstance(spec, dict):
        src = named.get(spec.get("from"))
        rel = spec.get("relationship", "success")
        hits = [c["id"] for c in connections if c["component"]["source"]["id"] == src
                and rel in (c["component"].get("selectedRelationships") or [])]
        if len(hits) != 1:
            raise ToolError(f"output {spec!r} matches {len(hits)} connections; expected exactly one")
        return hits[0]
    if spec in by_id:
        return spec
    target = named.get(spec)
    if target in by_id:
        return target
    feeding = [c["id"] for c in connections if c["component"]["destination"]["id"] == target
               and not c["component"]["source"].get("name", "").startswith(INJECTOR_PREFIX)]
    if len(feeding) != 1:
        raise ToolError(f"output {spec!r} must be a connection or a component with exactly one incoming connection "
                        f"(found {len(feeding)})")
    return feeding[0]


async def _first_flowfile(nifi: NiFiClient, connection_id: str, timeout: float) -> dict | None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        listing = await nifi.run_async_request(f"/flowfile-queues/{connection_id}/listing-requests", None, "listingRequest")
        if listing.get("flowFileSummaries"):
            uid = listing["flowFileSummaries"][0]["uuid"]
            ff = (await nifi.get(f"/flowfile-queues/{connection_id}/flowfiles/{uid}"))["flowFile"]
            resp = await nifi.request("GET", f"/flowfile-queues/{connection_id}/flowfiles/{uid}/content")
            return {"attributes": ff.get("attributes") or {}, "content": resp.content.decode("utf-8", errors="replace")}
        if asyncio.get_running_loop().time() > deadline:
            return None
        await asyncio.sleep(0.5)


async def _set_processor(nifi: NiFiClient, pid: str, state: str) -> None:
    p = await nifi.get(f"/processors/{pid}")
    await nifi.request("PUT", f"/processors/{pid}/run-status", json={"revision": _rev(p), "state": state})


async def run_tests(nifi: NiFiClient, group_id: str, case: str | None = None) -> dict:
    """Run the saved cases for a group. Stopped processors in the group (not the injectors) are started for
    the run and stopped again afterwards; each case's output queue is emptied before and after it."""
    group_name = (await nifi.get(f"/process-groups/{group_id}"))["component"]["name"]
    cases = load_cases(group_name)
    if case:
        cases = [c for c in cases if c["name"] == case]
        if not cases:
            raise ToolError(f"No case named {case!r} in {tests_dir() / group_name}")
    if not cases:
        return {"group": group_name, "tests_dir": str(tests_dir() / group_name), "cases": 0, "passed": 0, "failed": 0, "results": []}

    started: list[str] = []
    results = []
    try:
        for c in cases:
            named, connections = await _components(nifi, group_id)
            result: dict[str, Any] = {"case": c["name"], "file": c["_file"]}
            try:
                target = named.get(c["target"])
                if target is None:
                    raise ToolError(f"target {c['target']!r} is not a processor, port or funnel in {group_name}")
                out = _output_connection(c["output"], named, connections)
                await _drop_queue(nifi, out)
                inp = c.get("input") or {}
                attrs = {k: str(v) for k, v in (inp.get("attributes") or {}).items()}
                sent = await inject(nifi, target, str(inp.get("content", "")), attrs)
                if sent.get("validation_errors"):
                    raise ToolError(f"injector invalid: {sent['validation_errors']}")
                if not started:
                    procs = (await nifi.get(f"/process-groups/{group_id}/processors"))["processors"]
                    for p in procs:
                        if p["component"]["state"] == "STOPPED" and not p["component"]["name"].startswith(INJECTOR_PREFIX):
                            await _set_processor(nifi, p["id"], "RUNNING")
                            started.append(p["id"])
                ff = await _first_flowfile(nifi, out, float(c.get("timeout", 15)))
                if ff is None:
                    result.update(passed=False, diffs=[f"nothing reached the output queue within {c.get('timeout', 15)}s"])
                else:
                    diffs = check(c.get("expect") or {}, ff["attributes"], ff["content"])
                    result.update(passed=not diffs, **({"diffs": diffs, "actual_content": ff["content"][:500]} if diffs else {}))
                await _drop_queue(nifi, out)
            except ToolError as e:
                result.update(passed=False, error=str(e))
            results.append(result)
    finally:
        for pid in started:
            await _set_processor(nifi, pid, "STOPPED")
        for pid in started:
            await _wait(lambda pid=pid: _processor_idle(nifi, pid))
    passed = sum(r["passed"] for r in results)
    return {"group": group_name, "cases": len(results), "passed": passed, "failed": len(results) - passed, "results": results}


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def run_flow_tests(ctx: Context, group_id: str, case: str | None = None) -> dict:
        """Run the saved test cases in flow-tests/<group name>/ (YAML or JSON) and report pass/fail with diffs.

        A case: name, input {content, attributes}, target (component name or id to inject into), output
        (connection name/id, or the funnel/port/processor whose incoming queue is read), expect
        {attributes: {name: value, or null for absent}, content: {exact|contains|json_subset: ...}},
        optional timeout seconds. Stopped processors in the group are started for the run and stopped after.
        """
        return await run_tests(_nifi(ctx), group_id, case)
