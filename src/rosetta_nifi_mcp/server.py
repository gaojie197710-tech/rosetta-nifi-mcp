"""NiFi MCP server, phase 1: read-only exploration, bulletins, queues and provenance."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations

import os

from . import build, expression, paging, flowspec, flowtests, golden, guide, insight, layout, lint, parameters, registry
from mcp.server.mcpserver.exceptions import ToolError

from .client import NiFiClient

logging.getLogger("httpx").setLevel(logging.WARNING)

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
MAX_CONTENT_BYTES = 64 * 1024


@asynccontextmanager
async def lifespan(_: MCPServer) -> AsyncIterator[NiFiClient]:
    client = NiFiClient()
    try:
        yield client
    finally:
        await client.aclose()


mcp = MCPServer(
    "nifi",
    instructions=(
        "Tools for an Apache NiFi 2.x instance. Process group id 'root' is the top canvas. "
        "Before setting processor properties, call get_processor_schema instead of guessing names. "
        "When debugging a flow: get_bulletins -> list_queue -> get_flowfile_content -> search_provenance. "
        "Before designing or building a flow, read get_design_guide; after building, check_flow_design "
        "must report no warnings."
    ),
    lifespan=lifespan,
)


def _nifi(ctx: Context) -> NiFiClient:
    return ctx.request_context.lifespan_context


def _status_counts(snapshot: dict) -> dict:
    return {k: snapshot.get(k) for k in ("flowFilesIn", "flowFilesOut", "queued", "bytesQueued") if k in snapshot}


@mcp.tool(annotations=READ_ONLY)
async def get_about(ctx: Context) -> dict:
    """NiFi version, timezone and the identity this server is authenticated as."""
    nifi = _nifi(ctx)
    about = (await nifi.get("/flow/about"))["about"]
    user = await nifi.get("/flow/current-user")
    return {"version": about["version"], "timezone": about.get("timezone"), "identity": user["identity"]}


@mcp.tool(annotations=READ_ONLY)
async def get_flow(
    ctx: Context, process_group_id: str = "root",
    status: paging.Status | None = None, name: str | None = None, type: str | None = None,
    limit: int = paging.DEFAULT_LIMIT, offset: int = 0,
) -> dict:
    """Contents of one process group: child groups, processors, ports, connections with queue sizes.

    Large groups stay compact: `counts` summarises every processor, and at most `limit` processors and
    connections are listed (a `paging` note gives next_offset). Narrow the list with status (invalid,
    stopped, running, disabled, erroring, back_pressured), a name substring or a type substring;
    connections are then limited to those touching the listed processors. Validation errors are
    listed on filtered calls; unfiltered, invalid processors carry "invalid": true.
    """
    nifi = _nifi(ctx)
    flow = (await nifi.get(f"/flow/process-groups/{process_group_id}"))["processGroupFlow"]
    f = flow["flow"]
    procs = f["processors"]
    erroring: set[str] = set()
    if status == "erroring":
        board = (await nifi.get("/flow/bulletin-board", groupId=flow["id"], limit=200))["bulletinBoard"]
        erroring = {b.get("sourceId") for b in board.get("bulletins", []) if b.get("canRead")
                    and b["bulletin"].get("level") in ("ERROR", "WARNING")}
    flags = paging.processor_flags(procs, f["connections"], erroring)
    chosen = paging.select(procs, flags, status, name, type)
    filtered = bool(status or name or type)
    conns = f["connections"]
    if filtered:
        ids = {p["id"] for p in chosen}
        conns = [c for c in conns if {c["component"]["source"]["id"], c["component"]["destination"]["id"]} & ids]
    # Busiest queues first, so a truncated list still shows where FlowFiles are piling up.
    conns = sorted(conns, key=lambda c: -(c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued") or 0))
    hint = "call get_flow again with offset, or narrow with status/name/type; diagnose <id> for details"
    proc_page, proc_note = paging.page(chosen, limit, offset, "processors", hint)
    conn_page, conn_note = paging.page(conns, limit, offset, "connections", hint)
    out = {
        "id": flow["id"],
        "name": flow["breadcrumb"]["breadcrumb"]["name"],
        "parent_id": flow.get("parentGroupId"),
        "counts": {**paging.counts(procs, flags), "connections": len(f["connections"]),
                   "input_ports": len(f["inputPorts"]), "output_ports": len(f["outputPorts"]),
                   "process_groups": len(f["processGroups"])},
        "process_groups": [
            {"id": g["id"], "name": g["component"]["name"], "running": g.get("runningCount"),
             "stopped": g.get("stoppedCount"), "invalid": g.get("invalidCount"),
             "versioned": g["component"].get("versionControlInformation", {}).get("state")}
            for g in f["processGroups"]
        ][:max(limit, 100)],
        "processors": [
            {"id": p["id"], "name": p["component"]["name"], "type": p["component"]["type"],
             "state": p["component"]["state"],
             # Full errors only on a filtered listing; otherwise a flag keeps big groups compact.
             **(({"validation_errors": [e[:160] for e in p["component"]["validationErrors"][:2]]} if filtered else {"invalid": True})
                if p["component"].get("validationErrors") else {})}
            for p in proc_page
        ],
        "input_ports": [{"id": p["id"], "name": p["component"]["name"]} for p in f["inputPorts"]][:100],
        "output_ports": [{"id": p["id"], "name": p["component"]["name"]} for p in f["outputPorts"]][:100],
        "connections": [
            {"id": c["id"], **({"name": c["component"]["name"]} if c["component"].get("name") else {}),
             "source": c["component"]["source"]["name"], "destination": c["component"]["destination"]["name"],
             "relationships": c["component"].get("selectedRelationships"),
             "queued": c.get("status", {}).get("aggregateSnapshot", {}).get("queued")}
            for c in conn_page
        ],
    }
    if proc_note or conn_note:
        out["paging"] = [n for n in (proc_note, conn_note) if n]
    return out


@mcp.tool(annotations=READ_ONLY)
async def get_processor(ctx: Context, processor_id: str) -> dict:
    """Full configuration and current status of one processor (properties, scheduling, relationships)."""
    p = await _nifi(ctx).get(f"/processors/{processor_id}")
    c = p["component"]
    cfg = c["config"]
    return {
        "id": p["id"], "name": c["name"], "type": c["type"], "bundle": c["bundle"], "state": c["state"],
        "parent_group_id": c["parentGroupId"],
        "properties": cfg["properties"],
        "scheduling": {k: cfg.get(k) for k in ("schedulingStrategy", "schedulingPeriod", "concurrentlySchedulableTaskCount", "executionNode")},
        "auto_terminated": cfg.get("autoTerminatedRelationships"),
        "relationships": [r["name"] for r in c.get("relationships", [])],
        "validation_errors": c.get("validationErrors"),
        "status": _status_counts(p.get("status", {}).get("aggregateSnapshot", {})),
    }


@mcp.tool(annotations=READ_ONLY)
async def search_processor_types(ctx: Context, query: str, limit: int = 20) -> dict:
    """Find available processor types by name, tag or description (e.g. 'json', 'http', 'kafka')."""
    q = query.lower()
    types = (await _nifi(ctx).get("/flow/processor-types"))["processorTypes"]
    hits = [
        t for t in types
        if q in t["type"].lower() or q in (t.get("description") or "").lower() or any(q in tag.lower() for tag in t.get("tags", []))
    ]
    hits.sort(key=lambda t: (q not in t["type"].rsplit(".", 1)[-1].lower(), t["type"]))
    return {"processor_types": [
        {"type": t["type"], "bundle": t["bundle"], "description": (t.get("description") or "")[:300]}
        for t in hits[:limit]
    ]}


@mcp.tool(annotations=READ_ONLY)
async def get_processor_schema(ctx: Context, processor_type: str) -> dict:
    """Property definitions (names, defaults, allowed values, required) and relationships for a processor type.

    processor_type is the fully qualified class name, e.g. org.apache.nifi.processors.standard.SplitJson.
    """
    nifi = _nifi(ctx)
    types = (await nifi.get("/flow/processor-types"))["processorTypes"]
    match = next((t for t in types if t["type"] == processor_type), None)
    if match is None:
        raise ToolError(f"Unknown processor type {processor_type!r}; use search_processor_types first")
    b = match["bundle"]
    d = await nifi.get(f"/flow/processor-definition/{b['group']}/{b['artifact']}/{b['version']}/{processor_type}")
    return {
        "type": processor_type,
        "bundle": b,
        "description": d.get("typeDescription"),
        "properties": [
            {"name": name, "display_name": p.get("displayName"), "description": p.get("description"),
             "required": p.get("required"), "default": p.get("defaultValue"), "sensitive": p.get("sensitive"),
             "expression_language": p.get("expressionLanguageScope"),
             "allowed_values": [v.get("value") for v in p.get("allowableValues", [])] or None,
             "controller_service": (p.get("typeProvidedByValue") or {}).get("type")}
            for name, p in (d.get("propertyDescriptors") or {}).items()
        ],
        "relationships": [{"name": r["name"], "description": r.get("description")} for r in d.get("supportedRelationships", [])],
        "dynamic_properties": bool(d.get("supportsDynamicProperties")),
    }


@mcp.tool(annotations=READ_ONLY)
async def get_bulletins(ctx: Context, source_id: str | None = None, group_id: str | None = None, limit: int = 50) -> dict:
    """Recent bulletins (warnings/errors) from processors and services, optionally filtered by component or group."""
    params: dict[str, Any] = {"limit": limit}
    if source_id:
        params["sourceId"] = source_id
    if group_id:
        params["groupId"] = group_id
    board = (await _nifi(ctx).get("/flow/bulletin-board", **params))["bulletinBoard"]
    return {"bulletins": [
        {"time": b["bulletin"].get("timestamp"), "level": b["bulletin"].get("level"),
         "source": b["bulletin"].get("sourceName"), "source_id": b.get("sourceId"),
         "group_id": b.get("groupId"), "message": b["bulletin"].get("message")}
        for b in board.get("bulletins", []) if b.get("canRead")
    ]}


@mcp.tool(annotations=READ_ONLY)
async def search_flow(ctx: Context, query: str) -> dict:
    """Search the whole canvas for components by name, id, property value or comment."""
    r = (await _nifi(ctx).get("/flow/search-results", q=query))["searchResultsDTO"]
    return {
        kind: [{"id": x["id"], "name": x.get("name"), "group_id": x.get("groupId"), "matches": x.get("matches")} for x in items]
        for kind, items in r.items() if items
    }


@mcp.tool(annotations=READ_ONLY)
async def list_queue(ctx: Context, connection_id: str) -> dict:
    """List FlowFiles waiting in a connection's queue (first 100), with size and attributes summary."""
    listing = await _nifi(ctx).run_async_request(f"/flowfile-queues/{connection_id}/listing-requests", None, "listingRequest")
    return {
        "queue_size": listing.get("queueSize"),
        "flowfiles": [
            {"uuid": f["uuid"], "filename": f.get("filename"), "size": f.get("size"),
             "queued_ms": f.get("queuedDuration"), "penalized": f.get("penalized")}
            for f in listing.get("flowFileSummaries", [])
        ],
    }


@mcp.tool(annotations=READ_ONLY)
async def get_flowfile(ctx: Context, connection_id: str, flowfile_uuid: str, include_content: bool = True) -> dict:
    """Attributes and (up to 64 KB of) content of one queued FlowFile."""
    nifi = _nifi(ctx)
    ff = (await nifi.get(f"/flowfile-queues/{connection_id}/flowfiles/{flowfile_uuid}"))["flowFile"]
    out: dict[str, Any] = {"uuid": ff["uuid"], "size": ff.get("size"), "attributes": ff.get("attributes")}
    if include_content:
        resp = await nifi.request("GET", f"/flowfile-queues/{connection_id}/flowfiles/{flowfile_uuid}/content")
        data = resp.content[:MAX_CONTENT_BYTES]
        out["content"] = data.decode("utf-8", errors="replace")
        out["content_truncated"] = len(resp.content) > MAX_CONTENT_BYTES
    return out


@mcp.tool(annotations=READ_ONLY)
async def search_provenance(ctx: Context, component_id: str | None = None, flowfile_uuid: str | None = None, max_results: int = 50) -> dict:
    """Provenance events (what happened to FlowFiles), newest first, filtered by component and/or FlowFile UUID."""
    terms: dict[str, Any] = {}
    if component_id:
        terms["ProcessorID"] = {"value": component_id, "inverse": False}
    if flowfile_uuid:
        terms["FlowFileUUID"] = {"value": flowfile_uuid, "inverse": False}
    body = {"provenance": {"request": {"maxResults": max_results, "summarize": True, "incrementalResults": False, "searchTerms": terms}}}
    prov = await _nifi(ctx).run_async_request("/provenance", body, "provenance", timeout=30)
    return {"events": [
        {"id": e.get("id"), "time": e.get("eventTime"), "type": e.get("eventType"), "component": e.get("componentName"),
         "component_id": e.get("componentId"), "flowfile_uuid": e.get("flowFileUuid"), "size": e.get("fileSize"),
         "details": e.get("details")}
        for e in prov.get("results", {}).get("provenanceEvents", [])
    ]}


registry.register_read(mcp)
insight.register(mcp)
guide.register(mcp)
lint.register(mcp)

if os.environ.get("NIFI_MCP_ALLOW_WRITES", "").lower() in ("1", "true", "yes"):
    build.register(mcp)
    registry.register_write(mcp)
    layout.register(mcp)
    parameters.register(mcp)
    flowspec.register(mcp)
    flowtests.register(mcp)
    golden.register(mcp)
    expression.register(mcp)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
