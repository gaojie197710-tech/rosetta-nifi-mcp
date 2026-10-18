"""Nested build_flow end-to-end test: grid top level, a stacked pipeline of two http_stage groups against a
local HTTP API, the parameter context bound to every group, business codes from a parameter, and the
design check. Sends four FlowFiles and checks where each lands:

- ok      -> success, with the kept response attribute
- reject  -> rejected, error.code from the X-Error-Code header (422 is a business code)
- down    -> failure after 3 retries (500), error.code http_500
- auth    -> failure, not rejected (401 is technical), error.code http_401

Also checks that a bad nested spec is rejected before anything is created. Run with NIFI_MCP_ALLOW_WRITES=true;
E2E_KEEP=1 keeps the group and context for inspection.
"""

import asyncio
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

os.environ.setdefault("NIFI_MCP_ALLOW_WRITES", "true")

from mcp import Client  # noqa: E402

from rosetta_nifi_mcp.client import NiFiClient  # noqa: E402
from rosetta_nifi_mcp.server import mcp  # noqa: E402

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"


class Api(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        status, body, code = {"/ok": (200, {"id": "X1"}, ""), "/reject": (422, {}, "not_allowed"),
                              "/down": (500, {}, ""), "/auth": (401, {}, "")}.get(self.path, (404, {}, "no_route"))
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        if code:
            self.send_header("X-Error-Code", code)
            self.send_header("X-Error-Msg", f"{code} from test API")
        self.end_headers()
        self.wfile.write(raw)


def spec(port: int) -> dict:
    stage = {"prefix": "t", "business_codes": "#{CODES}", "retry_penalty": "1 sec"}
    pipeline = {
        "name": "20 · Pipeline", "layout": "stack", "input_ports": ["in"],
        "output_ports": ["success", "rejected", "failure", "ledger"],
        "groups": [
            {"name": "21 · First", "stage": {**stage, "stage": "first", "url": "#{API}/ok",
                                             "keep": {"t.id": "${stage.response:jsonPath('$.id')}"}}},
            {"name": "22 · Second", "stage": {**stage, "stage": "second", "url": "#{API}/${t.path}"}},
        ],
        "connections": [
            {"source": "in", "destination": "21 · First/in"},
            {"source": "21 · First/success", "destination": "22 · Second/in"},
            {"source": "22 · Second/success", "destination": "success"},
            *[{"source": f"{g}/{p}", "destination": p} for g in ("21 · First", "22 · Second")
              for p in ("rejected", "failure", "ledger")],
        ]}
    return {
        "name": "mcp-e2e-nested", "layout": "grid", "columns": 2,
        "parameter_context": {"name": "mcp-e2e-nested-params", "parameters": [
            {"name": "API", "value": f"http://127.0.0.1:{port}"}, {"name": "CODES", "value": "400|404|409|422"}]},
        "processors": [{"name": "Start", "type": UPDATE_ATTRIBUTE, "properties": {}}],
        "funnels": ["ok", "no", "broken", "trace"],
        "groups": [pipeline],
        "connections": [
            {"source": "Start", "destination": "20 · Pipeline/in", "relationships": ["success"]},
            {"source": "20 · Pipeline/success", "destination": "ok"},
            {"source": "20 · Pipeline/rejected", "destination": "no"},
            {"source": "20 · Pipeline/failure", "destination": "broken"},
            {"source": "20 · Pipeline/ledger", "destination": "trace"},
        ]}


async def main() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    nifi = NiFiClient()
    async with Client(mcp) as client:
        async def call(tool: str, /, expect_error: bool = False, **args):
            r = await client.call_tool(tool, args)
            text = "".join(getattr(c, "text", "") for c in r.content)
            if r.is_error != expect_error:
                raise RuntimeError(f"{tool} {'failed' if r.is_error else 'unexpectedly succeeded'}: {text[:1500]}")
            print(f"ok  {tool}{' (rejected as expected)' if expect_error else ''}")
            return text if expect_error else json.loads(text)

        # A bad nested spec is rejected before anything is created.
        before = len((await call("get_flow"))["process_groups"])
        bad = spec(port)
        bad["groups"][0]["connections"].append({"source": "21 · First/nope", "destination": "failure"})
        bad["groups"][0]["groups"].append({"name": "23 · Broken", "stage": {"stage": "b", "url": "x"},
                                           "processors": [{"name": "p", "type": UPDATE_ATTRIBUTE}]})
        msg = await call("build_flow", expect_error=True, **bad)
        for needle in ("no output port 'nope'", "23 · Broken"):
            assert needle in msg, (needle, msg)
        assert len((await call("get_flow"))["process_groups"]) == before, "bad spec created a group"

        flow = await call("build_flow", **spec(port))
        gid, cid = flow["group_id"], flow["parameter_context_id"]
        try:
            assert flow["design"]["clean"], flow["design"]
            assert set(flow["validation_errors"] or {}) <= {"Start"}, flow["validation_errors"]
            pipe = flow["groups"]["20 · Pipeline"]

            # The context is bound to every group, stage groups included.
            for g in [gid, pipe["group_id"], *(s["group_id"] for s in pipe["groups"].values())]:
                bound = (await nifi.get(f"/process-groups/{g}"))["component"].get("parameterContext")
                assert bound and bound["id"] == cid, (g, bound)

            for path in ("ok", "reject", "down", "auth"):
                await call("inject_test_flowfile", destination_id=flow["processors"]["Start"], content=path,
                           attributes={"t.path": path})
            await call("set_run_state", component_id=gid, state="RUNNING", kind="process_group")

            sinks = {c["destination"]: c["id"] for c in flow["connections"] if c["destination"] in ("ok", "no", "broken", "trace")}
            want = {"ok": 1, "no": 1, "broken": 2}
            for _ in range(60):
                sizes = {k: len((await call("list_queue", connection_id=sinks[k]))["flowfiles"]) for k in want}
                if sizes == want:
                    break
                await asyncio.sleep(1)
            assert sizes == want, sizes

            async def attrs(sink: str) -> list[dict]:
                q = await call("list_queue", connection_id=sinks[sink])
                return [(await call("get_flowfile", connection_id=sinks[sink], flowfile_uuid=f["uuid"],
                                    include_content=False))["attributes"] for f in q["flowfiles"]]

            [ok] = await attrs("ok")
            assert ok["t.id"] == "X1" and ok["t.stage"] == "second", ok
            [no] = await attrs("no")
            assert no["error.code"] == "not_allowed" and no["t.stage"] == "second", no
            broken = {a["t.path"]: a for a in await attrs("broken")}
            assert broken["down"]["error.code"] == "http_500", broken["down"]
            assert broken["auth"]["error.code"] == "http_401", broken["auth"]
            trace = len((await call("list_queue", connection_id=sinks["trace"]))["flowfiles"])
            assert trace == 8, f"ledger copies: {trace} (want 4 at each stage entry)"

            design = await call("check_flow_design", group_id=gid)
            assert design["clean"], design
            print("\nPASS: nested spec validated, stages routed ok / rejected / 500 / 401 correctly, "
                  "context on every group, no design warnings")
            return 0
        finally:
            if os.environ.get("E2E_KEEP"):
                print(f"    kept group {gid} and context {cid}")
            else:
                await call("delete_component", component_id=gid, kind="process_group")
                await call("delete_parameter_context", context_id=cid)
            server.shutdown()
            await nifi.aclose()


sys.exit(asyncio.run(main()))
