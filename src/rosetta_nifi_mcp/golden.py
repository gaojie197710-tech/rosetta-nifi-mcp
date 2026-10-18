"""Bulk golden-file replay: thousands of input/expected-output pairs replayed through a target component,
summarised as a pass rate with sample diffs, and written in full to the export directory as release evidence.

Layout, under flow-tests/<group name>/golden/<suite>/:
    suite.yaml         target, output, match (exact|json|json_subset|contains), threshold, batch_size, timeout
    inputs/NAME.*      content to send
    expected/NAME.*    expected output content (same NAME, any extension)
    attributes/NAME.json  optional attributes for that input
    expected_attributes/NAME.json  optional attributes the output must carry (null = must be absent)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import difflib
import hashlib
import json
import os
import re
import secrets
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import yaml
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from .build import CLIENT_ID, INJECTOR_PREFIX, WRITE, _bundle_for, _drop_queue, _nifi, _processor_idle, _rev, _wait
from .client import NiFiClient, NiFiError
from .flowtests import _components, check, _json_subset, _output_connection, _set_processor, tests_dir

CASE_ATTR = "mcp.golden.case"
LISTEN_HTTP = "org.apache.nifi.processors.standard.ListenHTTP"
MATCHES = ("exact", "json", "json_subset", "contains")
MAX_BATCH = 100  # NiFi's queue listing returns at most 100 FlowFiles
REGISTRY_PORT = 18080


def _suite_dirs(group_name: str) -> list[Path]:
    base = tests_dir().resolve()
    root = (base / group_name / "golden").resolve()
    if root.parent.parent != base:
        raise ToolError(f"Group name {group_name!r} cannot be used as a directory under {base}")
    return sorted(d for d in root.iterdir() if d.is_dir() and not d.is_symlink()) if root.is_dir() else []


def load_suite(d: Path) -> tuple[dict, list[dict]]:
    cfg = yaml.safe_load((d / "suite.yaml").read_text()) if (d / "suite.yaml").exists() else {}
    for key in ("target", "output"):
        if key not in cfg:
            raise ToolError(f"{d / 'suite.yaml'} needs {key!r}")
    if cfg.setdefault("match", "exact") not in MATCHES:
        raise ToolError(f"match must be one of {MATCHES}")

    def by_name(sub: str) -> dict[str, Path]:
        p = d / sub
        return {f.stem: f for f in sorted(p.iterdir()) if f.is_file() and not f.is_symlink()} if p.is_dir() else {}
    inputs, expected, attrs = by_name("inputs"), by_name("expected"), by_name("attributes")
    expected_attrs = by_name("expected_attributes")
    cases = []
    for name, f in inputs.items():
        cases.append({"name": name, "input": f, "expected": expected.get(name), "attributes": attrs.get(name),
                      "expected_attributes": expected_attrs.get(name)})
    return cfg, cases


def _suite_hash(cases: list[dict], cfg_text: str) -> str:
    h = hashlib.sha256(cfg_text.encode())
    for c in cases:
        for key in ("input", "expected", "attributes", "expected_attributes"):
            if c[key]:
                h.update(c[key].name.encode())
                h.update(c[key].read_bytes())
    return h.hexdigest()


def compare(mode: str, expected: str, actual: str) -> list[str]:
    if mode == "exact":
        if expected.rstrip() == actual.rstrip():
            return []
        return ["\n".join(list(difflib.unified_diff(expected.rstrip().splitlines(), actual.rstrip().splitlines(),
                                                    "expected", "actual", lineterm=""))[:40])]
    if mode == "contains":
        return [] if expected.strip() in actual else [f"output does not contain the expected text; got {actual[:300]!r}"]
    try:
        exp = json.loads(expected)
    except json.JSONDecodeError as e:
        return [f"expected file is not JSON: {e}"]
    try:
        act = json.loads(actual)
    except json.JSONDecodeError as e:
        return [f"output is not JSON ({e}): {actual[:300]!r}"]
    diffs = _json_subset(exp, act)
    if mode == "json" and not diffs and exp != act:
        diffs = [f"unexpected {d.split(': ', 1)[0]} in output" for d in _json_subset(act, exp)] or ["outputs differ"]
    return diffs[:20]


def _free_port() -> int:
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port != REGISTRY_PORT:
            return port


async def _listener(nifi: NiFiClient, group_id: str, target: str, attr_names: set[str]) -> tuple[str, str, int, str]:
    """A temporary ListenHTTP connected to the target; returns (processor id, connection id, port, base path).

    ListenHTTP cannot be bound to loopback only, so it listens on a random port under a random secret
    base path for the length of the replay; anything posted elsewhere gets a 404."""
    from .build import _find_connectable, CONNECTABLE

    kind, dst = await _find_connectable(nifi, target)
    port = _free_port()
    base_path = f"mcp-golden-{secrets.token_urlsafe(18)}"
    regex = "|".join(re.escape(a) for a in sorted(attr_names | {CASE_ATTR}))
    bundle = await _bundle_for(nifi, "/flow/processor-types", "processorTypes", LISTEN_HTTP)
    pos = dst.get("position") or dst["component"].get("position") or {"x": 0, "y": 0}
    p = await nifi.post(f"/process-groups/{group_id}/processors", {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {"type": LISTEN_HTTP, "bundle": bundle, "name": f"{INJECTOR_PREFIX}golden replay",
                      "position": {"x": pos["x"] - 450, "y": pos["y"] - 150},
                      "config": {"properties": {"Listening Port": str(port), "Base Path": base_path,
                                                "HTTP Headers for Attributes": regex}}}})
    c = await nifi.post(f"/process-groups/{group_id}/connections", {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {"source": {"id": p["id"], "groupId": group_id, "type": "PROCESSOR"},
                      "destination": {"id": target, "groupId": dst["component"]["parentGroupId"], "type": CONNECTABLE[kind]},
                      "selectedRelationships": ["success"], "backPressureObjectThreshold": 100000}})
    return p["id"], c["id"], port, base_path


async def _remove(nifi: NiFiClient, pid: str, cid: str) -> None:
    p = await nifi.get(f"/processors/{pid}")
    if p["component"]["state"] == "RUNNING":
        await _set_processor(nifi, pid, "STOPPED")
        await _wait(lambda: _processor_idle(nifi, pid))
    await _drop_queue(nifi, cid)
    c = await nifi.get(f"/connections/{cid}")
    await nifi.request("DELETE", f"/connections/{cid}", params={"version": c["revision"]["version"], "clientId": CLIENT_ID})
    p = await nifi.get(f"/processors/{pid}")
    await nifi.request("DELETE", f"/processors/{pid}", params={"version": p["revision"]["version"], "clientId": CLIENT_ID})


async def _collect(nifi: NiFiClient, out: str, want: int, timeout: float) -> list[dict]:
    """Wait until `want` FlowFiles are queued (or timeout), then read every listed one."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        listing = await nifi.run_async_request(f"/flowfile-queues/{out}/listing-requests", None, "listingRequest")
        summaries = listing.get("flowFileSummaries") or []
        if len(summaries) >= want or asyncio.get_running_loop().time() > deadline:
            break
        await asyncio.sleep(0.3)
    sem = asyncio.Semaphore(16)

    async def read(uid: str) -> dict:
        async with sem:
            ff = (await nifi.get(f"/flowfile-queues/{out}/flowfiles/{uid}"))["flowFile"]
            body = (await nifi.request("GET", f"/flowfile-queues/{out}/flowfiles/{uid}/content")).content
            return {"attributes": ff.get("attributes") or {}, "content": body.decode("utf-8", errors="replace")}
    return await asyncio.gather(*(read(s["uuid"]) for s in summaries))


async def replay_suite(nifi: NiFiClient, group_id: str, d: Path, batch_size: int | None = None,
                       max_cases: int | None = None, threshold: float | None = None) -> dict:
    cfg, cases = load_suite(d)
    if max_cases:
        cases = cases[:max_cases]
    if not cases:
        raise ToolError(f"No inputs in {d / 'inputs'}")
    batch = max(1, min(int(batch_size or cfg.get("batch_size", MAX_BATCH)), MAX_BATCH))
    timeout = float(cfg.get("timeout", 30))
    threshold = float(threshold if threshold is not None else cfg.get("threshold", 100.0))
    mode = cfg["match"]
    named, connections = await _components(nifi, group_id)
    target = named.get(cfg["target"])
    if target is None:
        raise ToolError(f"target {cfg['target']!r} is not a processor, port or funnel in the group")
    out = _output_connection(cfg["output"], named, connections)
    attrs_by_case = {c["name"]: {k: str(v) for k, v in json.loads(c["attributes"].read_text()).items()} if c["attributes"] else {}
                     for c in cases}
    attr_names = {k for a in attrs_by_case.values() for k in a}

    host = urlparse(os.environ.get("NIFI_URL", "https://localhost:9443")).hostname or "localhost"
    started: list[str] = []
    results: list[dict] = []
    t0 = time.monotonic()
    await _drop_queue(nifi, out)
    lid, lconn, port, base_path = await _listener(nifi, group_id, target, attr_names)
    cleanup_error = None
    try:
        await _set_processor(nifi, lid, "RUNNING")
        for p in (await nifi.get(f"/process-groups/{group_id}/processors"))["processors"]:
            if p["component"]["state"] == "STOPPED" and not p["component"]["name"].startswith(INJECTOR_PREFIX):
                await _set_processor(nifi, p["id"], "RUNNING")
                started.append(p["id"])
        url = f"http://{host}:{port}/{base_path}"
        async with httpx.AsyncClient(timeout=30) as http:
            for _ in range(40):  # the listener takes a moment to bind
                try:
                    await http.head(url)
                    break
                except httpx.TransportError:
                    await asyncio.sleep(0.25)
            sem = asyncio.Semaphore(20)

            async def send(c: dict) -> str | None:
                headers = {**attrs_by_case[c["name"]], CASE_ATTR: c["name"]}
                async with sem:
                    try:
                        r = await http.post(url, content=c["input"].read_bytes(), headers=headers)
                        return None if r.status_code < 300 else f"listener answered {r.status_code}"
                    except httpx.HTTPError as e:
                        return f"could not send: {e}"

            for i in range(0, len(cases), batch):
                chunk = cases[i:i + batch]
                send_errors = dict(zip((c["name"] for c in chunk), await asyncio.gather(*(send(c) for c in chunk))))
                outputs = await _collect(nifi, out, sum(e is None for e in send_errors.values()), timeout)
                await _drop_queue(nifi, out)
                by_case: dict[str, list[dict]] = {}
                for o in outputs:
                    by_case.setdefault(o["attributes"].get(CASE_ATTR, ""), []).append(o)
                for c in chunk:
                    r: dict[str, Any] = {"case": c["name"]}
                    got = by_case.get(c["name"]) or by_case.get(c["name"].lower())
                    if send_errors[c["name"]]:
                        r.update(passed=False, diffs=[send_errors[c["name"]]])
                    elif not got:
                        r.update(passed=False, diffs=[f"no output reached the output queue within {timeout}s"])
                    elif c["expected"] is None:
                        r.update(passed=False, diffs=["no expected file"])
                    else:
                        diffs = compare(mode, c["expected"].read_text(errors="replace"), got[0]["content"])
                        if c["expected_attributes"]:
                            diffs += check({"attributes": json.loads(c["expected_attributes"].read_text())},
                                           got[0]["attributes"], got[0]["content"])
                        if len(got) > 1:
                            diffs.append(f"{len(got)} outputs for one input")
                        r.update(passed=not diffs, **({"diffs": diffs, "actual": got[0]["content"][:500]} if diffs else {}))
                    results.append(r)
    finally:
        for pid in started:
            await _set_processor(nifi, pid, "STOPPED")
        for pid in started:
            await _wait(lambda pid=pid: _processor_idle(nifi, pid))
        try:
            await _remove(nifi, lid, lconn)
        except NiFiError as e:
            cleanup_error = (f"could not remove the temporary listener {lid} on port {port}; stop and delete it: "
                             f"{str(e)[:300]}")
        await _drop_queue(nifi, out)

    duration = time.monotonic() - t0
    passed = sum(r["passed"] for r in results)
    rate = round(100.0 * passed / len(results), 3)
    return {"suite": d.name, "match": mode, "cases": len(results), "passed": passed, "failed": len(results) - passed,
            "pass_rate": rate, "threshold": threshold, "meets_threshold": rate >= threshold,
            "duration_s": round(duration, 1), "cases_per_s": round(len(results) / duration, 1) if duration else None,
            "suite_sha256": _suite_hash(cases, (d / "suite.yaml").read_text() if (d / "suite.yaml").exists() else ""),
            **({"cleanup_error": cleanup_error} if cleanup_error else {}),
            "results": results}


async def replay(nifi: NiFiClient, group_id: str, suite: str | None = None, batch_size: int | None = None,
                 max_cases: int | None = None, threshold: float | None = None, failure_samples: int = 10,
                 write_report: bool = True) -> dict:
    from .registry import _export_path

    pg = await nifi.get(f"/process-groups/{group_id}")
    name = pg["component"]["name"]
    dirs = _suite_dirs(name)
    if suite:
        dirs = [d for d in dirs if d.name == suite]
        if not dirs:
            raise ToolError(f"No golden suite {suite!r} under {tests_dir() / name / 'golden'}")
    if not dirs:
        return {"group": name, "suites": [], "note": f"no golden suites under {tests_dir() / name / 'golden'}"}
    suites = [await replay_suite(nifi, group_id, d, batch_size, max_cases, threshold) for d in dirs]
    about = (await nifi.get("/flow/about"))["about"]
    vci = pg["component"].get("versionControlInformation") or {}
    evidence = {
        "kind": "E1 regression (golden replay)",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "nifi_version": about.get("version"), "group": name, "group_id": group_id,
        "flow_version": {k: vci.get(k) for k in ("flowName", "version", "state")} if vci else None,
    }
    summary = {**evidence, "meets_threshold": all(s["meets_threshold"] for s in suites), "suites": [
        {**{k: v for k, v in s.items() if k != "results"},
         "failure_sample": [r for r in s["results"] if not r["passed"]][:failure_samples]} for s in suites]}
    if write_report:
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
        path = _export_path(f"golden-{safe}-{stamp}.json")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump({**evidence, "meets_threshold": summary["meets_threshold"], "suites": suites}, fh, indent=1)
        summary["report_file"] = str(path)
    return summary


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def run_golden_replay(
        ctx: Context, group_id: str, suite: str | None = None, threshold: float | None = None,
        batch_size: int | None = None, max_cases: int | None = None, failure_samples: int = 10,
    ) -> dict:
        """Replay golden input/expected-output pairs from flow-tests/<group>/golden/<suite>/ through the group.

        Each suite has suite.yaml (target, output, match: exact|json|json_subset|contains, optional
        threshold %, batch_size <= 100, timeout s), inputs/NAME.*, expected/NAME.* and optional
        attributes/NAME.json. Inputs are POSTed to a temporary ListenHTTP connected to the target (removed
        afterwards) and outputs are matched back by the mcp.golden.case attribute. Returns per-suite pass
        rate, threshold verdict, timing and a sample of failures with diffs; the full per-case report is
        written to the export directory (report_file) as release evidence.
        """
        return await replay(_nifi(ctx), group_id, suite, batch_size, max_cases, threshold, failure_samples)
