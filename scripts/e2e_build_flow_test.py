"""build_flow + parameter context end-to-end test: rejects a bad spec, builds a two-processor flow bound to a
new parameter context, pushes test data through it, updates a parameter, checks the output again and
cleans up the group and the context. Run with NIFI_MCP_ALLOW_WRITES=true."""

import asyncio
import json
import os
import sys

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.server import mcp  # noqa: E402

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
REPLACE_TEXT = "org.apache.nifi.processors.standard.ReplaceText"
SECRET = "s3cr3t-never-echoed"


async def main() -> int:
    async with Client(mcp) as client:
        async def call(tool: str, /, expect_error: bool = False, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error != expect_error:
                raise RuntimeError(f"{tool} {'failed' if r.is_error else 'unexpectedly succeeded'}: {text[:800]}")
            print(f"ok  {tool}{' (rejected as expected)' if expect_error else ''}")
            return text if expect_error else json.loads(text)

        processors = [
            {"name": "tag", "type": UPDATE_ATTRIBUTE, "properties": {"greeting": "#{greeting}"}},
            {"name": "prefix", "type": REPLACE_TEXT, "auto_terminate": ["failure"],
             "properties": {"Replacement Strategy": "Prepend", "Replacement Value": "#{greeting}: ",
                            "Evaluation Mode": "Entire text"}},
        ]
        connections = [{"source": "tag", "destination": "prefix", "relationships": ["success"]},
                       {"source": "prefix", "destination": "out", "relationships": ["success"]}]
        sections = [{"title": "Ingest", "members": ["tag"]}, {"title": "Transform", "members": ["prefix", "out"]}]

        # A bad spec is rejected before anything is created.
        before = len((await call("get_flow"))["process_groups"])
        bad = [{**processors[1], "properties": {"Replacement Valu": "x", "Replacement Mode": "y"}}, processors[0]]
        msg = await call("build_flow", expect_error=True, name="mcp-e2e-bad", processors=bad,
                         connections=[*connections, {"source": "tag", "destination": "nowhere", "relationships": ["nope"]}],
                         funnels=["out"])
        for needle in ("Replacement Valu", "Replacement Mode", "nowhere", "nope"):
            assert needle in msg, (needle, msg)
        assert len((await call("get_flow"))["process_groups"]) == before, "bad spec created a group"

        ctx_name = "mcp-e2e-params"
        flow = await call("build_flow", name="mcp-e2e-build-flow", processors=processors, connections=connections,
                          funnels=["out"], sections=sections,
                          parameter_context={"name": ctx_name, "parameters": [
                              {"name": "greeting", "value": "hello"},
                              {"name": "api.token", "value": SECRET, "sensitive": True}]})
        gid, cid = flow["group_id"], flow["parameter_context_id"]
        try:
            # The entry processor waits for an upstream connection; inject_test_flowfile provides one.
            assert list(flow["validation_errors"] or {}) in ([], ["tag"]), flow["validation_errors"]
            assert SECRET not in json.dumps(flow)
            listed = await call("list_parameter_contexts")
            mine = next(c for c in listed["parameter_contexts"] if c["id"] == cid)
            assert SECRET not in json.dumps(listed) and gid in mine["bound_process_groups"], mine
            token = next(p for p in mine["parameters"] if p["name"] == "api.token")
            assert token["sensitive"] and "value" not in token, token

            out = next(c["id"] for c in flow["connections"] if c["destination"] == "out")

            async def run_once(content: str) -> dict:
                await call("inject_test_flowfile", destination_id=flow["processors"]["tag"], content=content)
                await call("set_run_state", component_id=gid, state="RUNNING", kind="process_group")
                for _ in range(30):
                    q = await call("list_queue", connection_id=out)
                    if q["flowfiles"]:
                        break
                    await asyncio.sleep(0.5)
                assert q["flowfiles"], "nothing reached the output queue"
                ff = await call("get_flowfile", connection_id=out, flowfile_uuid=q["flowfiles"][0]["uuid"])
                await call("set_run_state", component_id=gid, state="STOPPED", kind="process_group")
                await call("empty_queue", connection_id=out)
                return ff

            ff = await run_once("world")
            assert ff["content"] == "hello: world", ff["content"]
            assert ff["attributes"]["greeting"] == "hello", ff["attributes"]

            updated = await call("update_parameter_context", context_id=cid, parameters=[
                {"name": "greeting", "value": "bonjour"}, {"name": "api.token", "value": None}])
            assert [p["name"] for p in updated["parameters"]] == ["greeting"], updated

            ff = await run_once("monde")
            assert ff["content"] == "bonjour: monde", ff["content"]
            print("\nPASS: spec validated, flow built with parameters, output verified before and after update")
            return 0
        finally:
            await call("delete_component", component_id=gid, kind="process_group")
            await call("delete_parameter_context", context_id=cid)


sys.exit(asyncio.run(main()))
