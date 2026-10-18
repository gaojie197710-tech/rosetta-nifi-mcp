"""test_expression: evaluate Expression Language, RecordPath or Jolt through a temporary processor in a
hidden sandbox group that is created for the call and deleted afterwards."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from .build import CLIENT_ID, WRITE, _bundle_for, _delete_group, _nifi, _rev, _wait, inject
from .client import NiFiClient, NiFiError
from .insight import bulletins

SANDBOX_NAME = "[mcp] expression sandbox"
RESULT_ATTR = "mcp.expression.result"
RESULT_FIELD = "mcp_result"
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
UPDATE_RECORD = "org.apache.nifi.processors.standard.UpdateRecord"
JOLT = "org.apache.nifi.processors.jolt.JoltTransformJSON"
JSON_READER = "org.apache.nifi.json.JsonTreeReader"
JSON_WRITER = "org.apache.nifi.json.JsonRecordSetWriter"


async def _post(nifi: NiFiClient, path: str, component: dict) -> dict:
    return await nifi.post(path, {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": component})


async def _service(nifi: NiFiClient, gid: str, type_: str, properties: dict) -> str:
    bundle = await _bundle_for(nifi, "/flow/controller-service-types", "controllerServiceTypes", type_)
    s = await _post(nifi, f"/process-groups/{gid}/controller-services", {"type": type_, "bundle": bundle, "properties": properties})
    await nifi.request("PUT", f"/controller-services/{s['id']}/run-status", json={"revision": _rev(s), "state": "ENABLED"})

    async def enabled() -> bool:
        return (await nifi.get(f"/controller-services/{s['id']}"))["component"]["state"] == "ENABLED"
    if not await _wait(enabled):
        errs = (await nifi.get(f"/controller-services/{s['id']}"))["component"].get("validationErrors")
        raise ToolError(f"{type_.rsplit('.', 1)[-1]} did not enable: {errs}")
    return s["id"]


async def _sandbox(nifi: NiFiClient) -> str:
    flow = (await nifi.get("/flow/process-groups/root"))["processGroupFlow"]["flow"]
    for g in flow["processGroups"]:
        if g["component"]["name"] == SANDBOX_NAME:  # left over from an interrupted call
            await _delete_group(nifi, g["id"])
    g = await _post(nifi, "/process-groups/root/process-groups", {"name": SANDBOX_NAME, "position": {"x": -6000, "y": -6000}})
    return g["id"]


async def _first(nifi: NiFiClient, connection_id: str) -> dict | None:
    listing = await nifi.run_async_request(f"/flowfile-queues/{connection_id}/listing-requests", None, "listingRequest")
    if not listing.get("flowFileSummaries"):
        return None
    uid = listing["flowFileSummaries"][0]["uuid"]
    ff = (await nifi.get(f"/flowfile-queues/{connection_id}/flowfiles/{uid}"))["flowFile"]
    content = (await nifi.request("GET", f"/flowfile-queues/{connection_id}/flowfiles/{uid}/content")).content
    return {"attributes": ff.get("attributes") or {}, "content": content.decode("utf-8", errors="replace")}


async def evaluate(nifi: NiFiClient, language: str, expression: str, attributes: dict[str, str] | None,
                   content: str | None, timeout: float = 15) -> dict:
    if language in ("record_path", "jolt") and content is None:
        raise ToolError(f"{language} needs sample JSON in content")
    if language == "jolt":
        try:
            json.loads(expression)
        except json.JSONDecodeError as e:
            raise ToolError(f"Jolt spec is not valid JSON: {e}")
    gid = await _sandbox(nifi)
    try:
        if language == "expression_language":
            type_, props = UPDATE_ATTRIBUTE, {RESULT_ATTR: expression}
        elif language == "record_path":
            reader = await _service(nifi, gid, JSON_READER, {})
            writer = await _service(nifi, gid, JSON_WRITER, {})
            type_, props = UPDATE_RECORD, {"Record Reader": reader, "Record Writer": writer,
                                           "Replacement Value Strategy": "record-path-value",
                                           f"/{RESULT_FIELD}": expression}
        else:
            type_, props = JOLT, {"Jolt Specification": expression}
        bundle = await _bundle_for(nifi, "/flow/processor-types", "processorTypes", type_)
        p = await _post(nifi, f"/process-groups/{gid}/processors",
                        {"type": type_, "bundle": bundle, "name": "evaluate", "position": {"x": 0, "y": 0},
                         "config": {"properties": props}})
        queues = {}
        for i, rel in enumerate([r["name"] for r in p["component"].get("relationships", [])]):
            f = await _post(nifi, f"/process-groups/{gid}/funnels", {"position": {"x": 400 * i, "y": 300}})
            c = await _post(nifi, f"/process-groups/{gid}/connections", {
                "source": {"id": p["id"], "groupId": gid, "type": "PROCESSOR"},
                "destination": {"id": f["id"], "groupId": gid, "type": "FUNNEL"}, "selectedRelationships": [rel]})
            queues[rel] = c["id"]
        sent = await inject(nifi, p["id"], content or "-", attributes)  # GenerateFlowFile refuses empty text
        if sent.get("validation_errors"):
            raise ToolError(f"Test FlowFile could not be generated: {sent['validation_errors']}")
        p = await nifi.get(f"/processors/{p['id']}")
        errors = p["component"].get("validationErrors")
        if errors:
            return {"language": language, "ok": False, "error": errors}
        await nifi.request("PUT", f"/processors/{p['id']}/run-status", json={"revision": _rev(p), "state": "RUN_ONCE"})

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            for rel, cid in queues.items():
                ff = await _first(nifi, cid)
                if ff:
                    return _result(language, rel, ff, await bulletins(nifi, 5, sourceId=p["id"]))
            bulls = [b for b in await bulletins(nifi, 5, sourceId=p["id"]) if b["level"] == "ERROR"]
            if bulls:
                return {"language": language, "ok": False, "error": bulls[0]["message"]}
            await asyncio.sleep(0.5)
        return {"language": language, "ok": False, "error": f"no output within {timeout}s"}
    finally:
        try:
            await _delete_group(nifi, gid)
        except NiFiError:
            pass


def _result(language: str, rel: str, ff: dict, bulls: list[dict]) -> dict:
    if rel != "success":
        return {"language": language, "ok": False, "relationship": rel,
                "error": next((b["message"] for b in bulls), f"routed to {rel}")}
    if language == "expression_language":
        return {"language": language, "ok": True, "result": ff["attributes"].get(RESULT_ATTR)}
    try:
        data: Any = json.loads(ff["content"])
    except json.JSONDecodeError:
        return {"language": language, "ok": True, "result": ff["content"]}
    if language == "record_path":
        records = data if isinstance(data, list) else [data]
        values = [r.get(RESULT_FIELD) if isinstance(r, dict) else None for r in records]
        return {"language": language, "ok": True, "result": values[0] if len(values) == 1 else values}
    return {"language": language, "ok": True, "result": data}


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def test_expression(
        ctx: Context, expression: str | list | dict,
        language: Literal["expression_language", "record_path", "jolt"] = "expression_language",
        attributes: dict[str, str] | None = None, content: str | list | dict | None = None,
    ) -> dict:
        """Evaluate an expression in NiFi itself and return the result or NiFi's error message.

        expression_language: a NiFi EL expression such as ${name:toUpper()} against the given attributes
        (via UpdateAttribute). record_path: a RecordPath such as /order/id against sample JSON in content
        (via UpdateRecord; one value per record). jolt: a Jolt chain spec (JSON) applied to sample JSON in
        content (via JoltTransformJSON). content and a Jolt spec may be given as JSON text or as objects.
        Runs in a hidden sandbox group that is deleted after the call.
        """
        if not isinstance(expression, str):
            expression = json.dumps(expression)
        if content is not None and not isinstance(content, str):
            content = json.dumps(content)
        return await evaluate(_nifi(ctx), language, expression, attributes, content)
