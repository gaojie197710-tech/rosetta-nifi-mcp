"""Golden replay end-to-end: builds the sandbox flow for flow-tests/mcp-flowtest-sandbox/, replays the repo's sample
suite (all pass), then replays a generated suite of 2,000 pairs with 10 wrong expectations (99.5%) and checks the
pass rate, threshold verdict, compact summary, full report file and the commit_version golden gate.
Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")
os.environ["NIFI_MCP_EXPORT_DIR"] = tempfile.mkdtemp()

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

GROUP = "mcp-flowtest-sandbox"
REPO_TESTS = Path(__file__).resolve().parents[1] / "flow-tests"
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
REPLACE_TEXT = "org.apache.nifi.processors.standard.ReplaceText"
N, WRONG = 2000, 10


def generate(root: Path) -> None:
    d = root / GROUP / "golden" / "bulk"
    for sub in ("inputs", "expected", "attributes"):
        (d / sub).mkdir(parents=True)
    (d / "suite.yaml").write_text("target: tag\noutput: {from: wrap}\nmatch: json\nthreshold: 99.5\n")
    for i in range(N):
        name = f"case-{i:05d}"
        (d / "inputs" / f"{name}.json").write_text(json.dumps({"n": i, "items": list(range(i % 5))}))
        want = {"wrapped": {"n": i, "items": list(range(i % 5))}, "by": "hello"}
        if i % (N // WRONG) == 7:
            want["by"] = "goodbye"
        (d / "expected" / f"{name}.json").write_text(json.dumps(want))
        if i % 3 == 0:
            (d / "attributes" / f"{name}.json").write_text(json.dumps({"order.id": str(i)}))


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, expect_error: bool = False, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error != expect_error:
                raise RuntimeError(f"{tool} {'failed' if r.is_error else 'unexpectedly succeeded'}: {text[:1500]}")
            print(f"ok  {tool}{' (rejected as expected)' if expect_error else ''} -> {len(text)} chars")
            return text if expect_error else json.loads(text)

        flow = await call("build_flow", name=GROUP, funnels=["out"], processors=[
            {"name": "tag", "type": UPDATE_ATTRIBUTE,
             "properties": {"order.checked": "${order.id:isEmpty():not()}", "greeting": "hello"}},
            {"name": "wrap", "type": REPLACE_TEXT, "auto_terminate": ["failure"],
             "properties": {"Replacement Strategy": "Regex Replace", "Evaluation Mode": "Entire text",
                            "Replacement Value": '{"wrapped": $1, "by": "${greeting}"}'}},
        ], connections=[{"source": "tag", "destination": "wrap", "relationships": ["success"]},
                        {"source": "wrap", "destination": "out", "relationships": ["success"]}])
        gid = flow["group_id"]
        tmp = Path(tempfile.mkdtemp())
        try:
            r = await call("run_golden_replay", group_id=gid)
            s = r["suites"][0]
            assert s["suite"] == "wrap" and s["cases"] == 5 and s["pass_rate"] == 100.0, json.dumps(r, indent=1)
            assert Path(r["report_file"]).exists() and r["nifi_version"] and r["kind"].startswith("E1")

            shutil.copytree(REPO_TESTS / GROUP, tmp / GROUP)
            generate(tmp)
            os.environ["NIFI_MCP_FLOW_TESTS_DIR"] = str(tmp)
            r = await call("run_golden_replay", group_id=gid, suite="bulk")
            s = r["suites"][0]
            print(f"    {s['cases']} cases, {s['pass_rate']}% in {s['duration_s']}s ({s['cases_per_s']}/s)")
            assert s["cases"] == N and s["failed"] == WRONG and s["pass_rate"] == 99.5 and s["meets_threshold"], s
            assert len(s["failure_sample"]) == 10 and all("goodbye" in json.dumps(f["diffs"]) for f in s["failure_sample"]), s["failure_sample"][:2]
            assert len(json.dumps(r)) < 15000, "summary is not compact"
            report = json.loads(Path(r["report_file"]).read_text())
            assert len(report["suites"][0]["results"]) == N, "full report incomplete"

            r = await call("run_golden_replay", group_id=gid, suite="bulk", threshold=99.6, max_cases=500)
            s = r["suites"][0]  # cases 7, 207 and 407 are wrong: 497/500
            assert s["cases"] == 500 and s["pass_rate"] == 99.4 and s["threshold"] == 99.6 and not r["meets_threshold"], s

            # Commit gate: wrap and bulk both pass at 99.5; 99.9 refuses. registry/bucket are never reached.
            msg = await call("commit_version", expect_error=True, group_id=gid, comment="refused",
                             registry_id="unused", bucket_id="unused", require_golden_pass_rate=99.9)
            assert "golden pass rate below 99.9" in msg and "'bulk': 99.5" in msg, msg[:500]
            print("\nPASS: golden replay reports pass rate, sample diffs and full evidence; commit gate enforces the threshold")
            return 0
        finally:
            os.environ.pop("NIFI_MCP_FLOW_TESTS_DIR", None)
            shutil.rmtree(tmp, ignore_errors=True)
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
