"""Saved flow tests end-to-end: builds the sandbox flow the sample suite in flow-tests/mcp-flowtest-sandbox/
describes, runs the suite (all pass), runs one case by name, then adds a failing case in a temporary copy of
the suite and checks it is reported with a diff and that commit_version(require_tests_pass) refuses.
Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

GROUP = "mcp-flowtest-sandbox"
REPO_TESTS = Path(__file__).resolve().parents[1] / "flow-tests"
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
REPLACE_TEXT = "org.apache.nifi.processors.standard.ReplaceText"


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, expect_error: bool = False, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error != expect_error:
                raise RuntimeError(f"{tool} {'failed' if r.is_error else 'unexpectedly succeeded'}: {text[:1500]}")
            print(f"ok  {tool}{' (rejected as expected)' if expect_error else ''}")
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
            report = await call("run_flow_tests", group_id=gid)
            assert report["cases"] == 3 and report["failed"] == 0, json.dumps(report, indent=1)

            one = await call("run_flow_tests", group_id=gid, case="order without id is not checked")
            assert one["cases"] == 1 and one["passed"] == 1, one
            procs = (await call("get_flow", process_group_id=gid))["processors"]
            # Nothing left running; injectors rest disabled so starting the group cannot fire them.
            assert all(p["state"] == ("DISABLED" if p["name"].startswith("[mcp-test] inject") else "STOPPED")
                       for p in procs), procs

            shutil.copytree(REPO_TESTS / GROUP, tmp / GROUP)
            (tmp / GROUP / "broken.yaml").write_text(
                "name: expects the wrong greeting\ninput: {content: '1'}\ntarget: tag\n"
                "output: {from: wrap}\nexpect:\n  attributes: {greeting: bye}\n  content: {exact: 'nope'}\n")
            os.environ["NIFI_MCP_FLOW_TESTS_DIR"] = str(tmp)
            report = await call("run_flow_tests", group_id=gid)
            assert report["failed"] == 1 and report["passed"] == 3, report
            bad = next(r for r in report["results"] if not r["passed"])
            assert any("greeting" in d and "'bye'" in d for d in bad["diffs"]), bad
            assert any(d.startswith("content differs") and "+{\"wrapped\": 1" in d for d in bad["diffs"]), bad

            msg = await call("commit_version", expect_error=True, group_id=gid, comment="should be refused",
                             registry_id="unused", bucket_id="unused", require_tests_pass=True)
            assert "Commit refused" in msg and "expects the wrong greeting" in msg, msg
            print("\nPASS: saved flow tests pass, failures carry diffs, commit is refused on failure")
            return 0
        finally:
            os.environ.pop("NIFI_MCP_FLOW_TESTS_DIR", None)
            shutil.rmtree(tmp, ignore_errors=True)
            await call("delete_component", component_id=gid, kind="process_group")


sys.exit(asyncio.run(main()))
