"""Read-only diagnosis tools: diagnose one processor or the problem spots of a group."""

from __future__ import annotations

from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from .client import NiFiClient, NiFiError

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
PREVIEW_BYTES = 300
FULL_PERCENT = 80


def _nifi(ctx: Context) -> NiFiClient:
    return ctx.request_context.lifespan_context


async def _try_get(nifi: NiFiClient, path: str, **params: Any) -> Any | None:
    try:
        return await nifi.get(path, **params)
    except NiFiError as e:
        if "404" in str(e):
            return None
        raise


def queue_summary(c: dict) -> dict:
    """Compact queue state of a connection entity, with back-pressure status."""
    s = c.get("status", {}).get("aggregateSnapshot", {})
    comp = c["component"]
    pct = max(s.get("percentUseCount") or 0, s.get("percentUseBytes") or 0)
    out = {"id": c["id"], "from": comp["source"].get("name"), "to": comp["destination"].get("name"),
           "relationships": comp.get("selectedRelationships"), "queued": s.get("queued"),
           "back_pressure_percent": pct}
    if pct >= 100:
        out["back_pressure"] = "ACTIVE"
    elif pct >= FULL_PERCENT:
        out["back_pressure"] = "NEAR"
    return out


async def group_connections(nifi: NiFiClient, group_id: str, recursive: bool = False) -> list[dict]:
    flow = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]["flow"]
    conns = list(flow["connections"])
    if recursive:
        for g in flow["processGroups"]:
            conns += await group_connections(nifi, g["id"], True)
    return conns


async def bulletins(nifi: NiFiClient, limit: int = 10, **params: Any) -> list[dict]:
    board = (await nifi.get("/flow/bulletin-board", limit=limit, **params))["bulletinBoard"]
    return [{"time": b["bulletin"].get("timestamp"), "level": b["bulletin"].get("level"),
             "source": b["bulletin"].get("sourceName"), "source_id": b.get("sourceId"),
             "message": (b["bulletin"].get("message") or "")[:400]}
            for b in board.get("bulletins", []) if b.get("canRead")]


async def _provenance(nifi: NiFiClient, component_id: str, n: int) -> list[dict]:
    body = {"provenance": {"request": {"maxResults": n, "summarize": True, "incrementalResults": False,
                                       "searchTerms": {"ProcessorID": {"value": component_id, "inverse": False}}}}}
    try:
        prov = await nifi.run_async_request("/provenance", body, "provenance", timeout=20)
    except NiFiError as e:
        return [{"error": str(e)[:200]}]
    events = []
    for e in prov.get("results", {}).get("provenanceEvents", [])[:n]:
        ev = {"time": e.get("eventTime"), "type": e.get("eventType"), "flowfile_uuid": e.get("flowFileUuid"),
              "size": e.get("fileSize")}
        if e.get("details"):
            ev["details"] = e["details"][:200]
        try:
            full = (await nifi.get(f"/provenance-events/{e['id']}"))["provenanceEvent"]
            side = "output" if full.get("outputContentAvailable") else "input" if full.get("inputContentAvailable") else None
            if side:
                resp = await nifi.request("GET", f"/provenance-events/{e['id']}/content/{side}")
                ev["content_preview"] = resp.content[:PREVIEW_BYTES].decode("utf-8", errors="replace")
        except NiFiError:
            pass
        events.append(ev)
    return events


async def diagnose_processor(nifi: NiFiClient, p: dict, provenance: int = 3) -> dict:
    c = p["component"]
    gid = c["parentGroupId"]
    conns = (await nifi.get(f"/process-groups/{gid}/connections"))["connections"]
    incoming = [x for x in conns if x["component"]["destination"]["id"] == p["id"]]
    outgoing = [x for x in conns if x["component"]["source"]["id"] == p["id"]]
    snap = p.get("status", {}).get("aggregateSnapshot", {})
    out: dict[str, Any] = {
        "id": p["id"], "name": c["name"], "type": c["type"].rsplit(".", 1)[-1], "state": c["state"],
        "validation": c.get("validationStatus"), "validation_errors": c.get("validationErrors") or [],
        "active_threads": snap.get("activeThreadCount"),
        "in_out_5min": f"{snap.get('input')} -> {snap.get('output')}",
        "incoming": [queue_summary(x) for x in incoming],
        "outgoing": [queue_summary(x) for x in outgoing],
        "upstream": sorted({x["component"]["source"].get("name") for x in incoming}),
        "downstream": sorted({x["component"]["destination"].get("name") for x in outgoing}),
        "unconnected_relationships": [
            r["name"] for r in c.get("relationships", [])
            if not r.get("autoTerminate") and not any(r["name"] in (x["component"].get("selectedRelationships") or []) for x in outgoing)],
        "bulletins": await bulletins(nifi, 5, sourceId=p["id"]),
    }
    if provenance:
        out["provenance"] = await _provenance(nifi, p["id"], provenance)
    return {k: v for k, v in out.items() if v not in ([], None)}


async def diagnose_group(nifi: NiFiClient, group_id: str, provenance: int = 2, status: str | None = None,
                         name: str | None = None, type_: str | None = None, limit: int = 20, offset: int = 0,
                         detail: int = 3) -> dict:
    """Problem components of a group and its descendants: a compact line for each (paged), full detail
    (queues, neighbours, bulletins, provenance) for the first `detail` of them."""
    from . import paging

    pg = (await nifi.get(f"/process-groups/{group_id}"))
    procs = (await nifi.get(f"/process-groups/{group_id}/processors", includeDescendantGroups="true"))["processors"]
    conns = await group_connections(nifi, group_id, recursive=True)
    bulls = await bulletins(nifi, 100, groupId=group_id)
    erroring = {b["source_id"] for b in bulls if b["level"] in ("ERROR", "WARNING", "WARN")}
    flags = paging.processor_flags(procs, conns, erroring)
    # Only the downstream end of a full queue is a problem (the upstream one is merely held back).
    blocked = {c["component"]["destination"]["id"] for c in conns if queue_summary(c).get("back_pressure")}
    problem_kinds = ("invalid", "erroring", "back_pressured")
    for p in procs:
        if "back_pressured" in flags[p["id"]] and p["id"] not in blocked:
            flags[p["id"]].discard("back_pressured")
    problems = [p for p in procs if flags[p["id"]] & set(problem_kinds)]
    chosen = paging.select(problems, flags, status, name, type_)
    hint = "diagnose <processor id> for full detail; page with offset or narrow with status/name/type"
    listed, note = paging.page(chosen, limit, offset, "problem_components", hint)
    pressured = [queue_summary(c) for c in conns]
    pressured = [q for q in pressured if q.get("back_pressure")]
    sc = pg.get("status", {}).get("aggregateSnapshot", {})
    out: dict[str, Any] = {
        "group": {"id": group_id, "name": pg["component"]["name"], "running": pg.get("runningCount"),
                  "stopped": pg.get("stoppedCount"), "invalid": pg.get("invalidCount"),
                  "disabled": pg.get("disabledCount"), "queued": sc.get("queued")},
        "problem_counts": {k: sum(k in flags[p["id"]] for p in problems) for k in problem_kinds},
        "back_pressured_queues": pressured[:10],
        "problem_components": [
            {"id": p["id"], "name": p["component"]["name"], "type": short_type(p["component"]["type"]),
             "state": p["component"]["state"], "problems": sorted(flags[p["id"]] & set(problem_kinds)),
             **({"first_error": (p["component"]["validationErrors"] or [""])[0][:200]} if p["component"].get("validationErrors") else {}),
             **({"last_bulletin": b["message"][:200]} if (b := next((x for x in bulls if x["source_id"] == p["id"]), None)) else {})}
            for p in listed],
        "details": [await diagnose_processor(nifi, p, provenance) for p in listed[:detail]],
        "other_bulletins": [b for b in bulls if b["source_id"] not in {p["id"] for p in problems}][:5],
        "healthy": not (problems or pressured or bulls),
    }
    if len(pressured) > 10:
        out["back_pressured_queues_total"] = len(pressured)
    if note:
        out["paging"] = note
    return {k: v for k, v in out.items() if v != []}


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def diagnose(
        ctx: Context, component_id: str, provenance_events: int = 3,
        status: Literal["invalid", "erroring", "back_pressured"] | None = None, name: str | None = None,
        type: str | None = None, limit: int = 20, offset: int = 0, detail: int = 3,
    ) -> dict:
        """One-call diagnosis of a processor or a process group.

        Processor: run state, validation errors, recent bulletins, incoming/outgoing queues with
        back-pressure status, neighbour names, unconnected relationships and the last provenance
        events with a short content preview. Process group: the same for its problem components only
        (invalid, erroring, or fed by a full queue), plus back-pressured queues anywhere inside it:
        problem_counts, a compact line per problem (at most `limit`, paged with offset, narrowed by
        status/name/type) and full detail for the first `detail` of them.
        """
        nifi = _nifi(ctx)
        p = await _try_get(nifi, f"/processors/{component_id}")
        if p:
            return await diagnose_processor(nifi, p, provenance_events)
        if component_id == "root" or await _try_get(nifi, f"/process-groups/{component_id}"):
            return await diagnose_group(nifi, component_id, min(provenance_events, 2), status, name, type,
                                        limit, offset, detail)
        raise ToolError(f"No processor or process group with id {component_id}")

    @mcp.tool(annotations=READ_ONLY)
    async def describe_flow(ctx: Context, group_id: str, markdown_file: str | None = None,
                            name: str | None = None, type: str | None = None,
                            limit: int = 25, offset: int = 0) -> dict:
        """Structured plain-language description of a process group, built from its actual configuration:
        inputs (listeners, ports, generators, with their ports/paths), steps in flow order with their
        non-default settings and routes, outputs, external endpoints, controller services and the
        parameter context. Child groups are described the same way.

        With markdown_file (a plain name ending in .md), also writes it as Markdown to the export
        directory (NIFI_MCP_EXPORT_DIR, default ~/.nifi-mcp/exports); existing files are never overwritten.
        The Markdown is always complete; the reply is compact for big groups: step counts by type, at
        most `limit` steps / inputs / outputs / endpoints (steps paged with offset and narrowed by a name
        or type substring) and child groups summarised by name and size.
        """
        import os

        from .registry import _export_path

        d = await describe(_nifi(ctx), group_id)
        if markdown_file:
            path = _export_path(markdown_file, ".md")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as fh:
                fh.write(to_markdown(d))
            d["markdown_file"] = str(path)
        return compact_description(d, limit, offset, name, type)

    @mcp.tool(annotations=READ_ONLY)
    async def health_check(ctx: Context, limit: int = 15) -> dict:
        """Canvas-wide health summary: invalid and stopped components per group, back-pressured queues,
        error bulletins, version-control state (SYNC_FAILURE and local drift flagged), registry client
        reachability, JVM heap and repository disk usage, and the listening ports configured in
        processors with conflicts (two listeners on one port, or one on the NiFi Registry's port 18080),
        certificate expiry (server certificate from the TLS handshake, this server's client
        certificate, its CA bundle, NIFI_MCP_EXTRA_CERTS; SSL context service stores are listed, not read):
        warning under 30 days, critical under 7. Stores that need a password are skipped and listed.
        `design` holds check_flow_design's warnings for the whole canvas (missing parameter contexts,
        parameters in quoted EL, 30 s InvokeHTTP retry penalties, overlapping connections, undocumented
        auto-terminated failures).
        `issues` lists everything that needs attention; `healthy` is true when it is empty. Lists hold
        at most `limit` entries (worst first) with totals alongside; version_control lists only groups
        that are not UP_TO_DATE.
        """
        return await health(_nifi(ctx), limit=limit)


# ---------- describe_flow ----------

INPUT_PREFIXES = ("Listen", "Consume", "Get", "List", "Generate", "HandleHttpRequest", "QueryDatabaseTable", "Tail", "Capture")
OUTPUT_PREFIXES = ("Put", "Publish", "InvokeHTTP", "PostHTTP", "HandleHttpResponse", "Send", "Delete", "Execute")
ENDPOINT_WORDS = ("url", "uri", "host", "port", "path", "directory", "topic", "bootstrap", "endpoint", "bucket",
                  "queue", "database", "table", "address", "brokers", "server")


def short_type(t: str) -> str:
    return t.rsplit(".", 1)[-1]


def endpoint_properties(props: dict[str, Any], descriptors: dict | None = None) -> dict[str, str]:
    """Properties that name something outside NiFi (URLs, hosts, ports, paths, topics), sensitive ones skipped."""
    out = {}
    for k, v in props.items():
        d = (descriptors or {}).get(k, {})
        if v in (None, "") or d.get("sensitive") or d.get("identifiesControllerService"):
            continue
        kl = k.lower()
        if any(w in kl for w in ENDPOINT_WORDS) and not any(w in kl for w in ("character set", "size", "timeout", "strategy", "json", "behavior", "expression", "record path")) \
                and len(str(v)) < 300:
            out[k] = v
    return out


def _configured(c: dict, services: dict[str, str], limit: int = 6) -> dict[str, str]:
    """Properties set to something other than their default, values shortened, service ids shown by name."""
    cfg = c["config"]
    descs = cfg.get("descriptors") or {}
    out = {}
    for k, v in cfg["properties"].items():
        d = descs.get(k, {})
        if v in (None, "") or v == d.get("defaultValue"):
            continue
        if v in services:
            v = f"service '{services[v]}'"
        out[k] = "(sensitive)" if d.get("sensitive") else (v if len(v) <= 80 else v[:77] + "...")
        if len(out) >= limit:
            break
    return out


async def describe(nifi: NiFiClient, group_id: str, depth: int = 0) -> dict:
    from .build import INJECTOR_PREFIX

    pgf = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]
    flow = pgf["flow"]
    pg = await nifi.get(f"/process-groups/{group_id}")
    procs = {p["id"]: p["component"] for p in flow["processors"] if not p["component"]["name"].startswith(INJECTOR_PREFIX)}
    names = {**{i: c["name"] for i, c in procs.items()},
             **{p["id"]: f"input port '{p['component']['name']}'" for p in flow["inputPorts"]},
             **{p["id"]: f"output port '{p['component']['name']}'" for p in flow["outputPorts"]},
             **{f["id"]: "funnel" for f in flow["funnels"]},
             **{g["id"]: f"group '{g['component']['name']}'" for g in flow["processGroups"]}}
    edges = [(c["component"]["source"]["id"], c["component"]["destination"]["id"], c["component"].get("selectedRelationships") or [],
              c["component"]["source"].get("groupId"), c["component"]["destination"].get("groupId"))
             for c in flow["connections"]]
    # Ports inside child groups are referenced by their group.
    def node(cid: str, gid: str | None) -> str:
        return cid if cid in names else gid if gid in names else cid
    edges = [(node(s, sg), node(d, dg), r) for s, d, r, sg, dg in edges if s in procs or s in names or sg in names]
    has_in = {d for s, d, _ in edges if s in names}
    has_out = {s for s, d, _ in edges if d in names}

    inputs, outputs = [], []
    for p in flow["inputPorts"]:
        inputs.append({"kind": "input port", "name": p["component"]["name"]})
    for pid, c in procs.items():
        t = short_type(c["type"])
        ep = endpoint_properties(c["config"]["properties"], c["config"].get("descriptors"))
        entry = {"name": c["name"], "type": t, **({"endpoints": ep} if ep else {})}
        if t.startswith(INPUT_PREFIXES) or pid not in has_in:
            inputs.append({"kind": "source", **entry})
        if t.startswith(OUTPUT_PREFIXES) or (pid not in has_out and not t.startswith(INPUT_PREFIXES)):
            outputs.append({"kind": "sink", **entry})
    for p in flow["outputPorts"]:
        outputs.append({"kind": "output port", "name": p["component"]["name"]})

    # Steps in flow order: breadth-first from the sources, then anything unreached.
    order: list[str] = []
    frontier = [i for i in names if i not in has_in]
    while frontier:
        nxt = []
        for n in frontier:
            if n not in order:
                order.append(n)
                nxt += [d for s, d, _ in edges if s == n]
        frontier = nxt
    order += [i for i in names if i not in order]
    services = (await nifi.get(f"/flow/process-groups/{group_id}/controller-services",
                               includeAncestorGroups="true", includeDescendantGroups="false"))["controllerServices"]
    service_names = {x["id"]: x["component"]["name"] for x in services}
    services = [x for x in services if x["component"].get("parentGroupId") == group_id]
    steps = []
    for n in order:
        if n not in procs:
            continue
        c = procs[n]
        routes = {}
        for s, d, rels in edges:
            if s == n:
                for r in rels:
                    routes.setdefault(r, []).append(names.get(d, d))
        steps.append({"name": c["name"], "type": short_type(c["type"]), "state": c["state"],
                      **({"config": cfg} if (cfg := _configured(c, service_names)) else {}),
                      **({"routes": routes} if routes else {}),
                      **({"auto_terminated": c["config"]["autoTerminatedRelationships"]}
                         if c["config"].get("autoTerminatedRelationships") else {})})

    def ep_line(name: str, ep: dict) -> str:
        return f"{name}: " + ", ".join(f"{k}={v}" for k, v in ep.items())
    endpoints = [ep_line(p["name"], ep) for p in procs.values()
                 if (ep := endpoint_properties(p["config"]["properties"], p["config"].get("descriptors")))]
    svc = []
    for s in services:
        sc = s["component"]
        ep = endpoint_properties(sc.get("properties") or {}, sc.get("descriptors"))
        if ep:
            endpoints.append(ep_line(f"service '{sc['name']}'", ep))
        svc.append({"name": sc["name"], "type": short_type(sc["type"]), "state": sc["state"], **({"endpoints": ep} if ep else {})})
    pc = pg["component"].get("parameterContext")
    out: dict[str, Any] = {
        "id": group_id,
        "name": pg["component"]["name"],
        "comments": pg["component"].get("comments") or None,
        "parameter_context": pc["component"]["name"] if pc and pc.get("component") else None,
        "inputs": inputs, "steps": steps, "outputs": outputs,
        "external_endpoints": endpoints, "controller_services": svc,
    }
    if flow["processGroups"] and depth < 3:
        out["child_groups"] = [await describe(nifi, g["id"], depth + 1) for g in flow["processGroups"]]
    return {k: v for k, v in out.items() if v not in (None, [], {})}


def to_markdown(d: dict, level: int = 1) -> str:
    h = "#" * min(level, 6)
    lines = []
    if level == 1:
        lines += ["---", f"title: {d['name']}", f"description: Generated description of the NiFi process group {d['name']}", "---", ""]
    lines.append(f"{h} {d['name']}\n")
    if d.get("comments"):
        lines.append(d["comments"] + "\n")
    if d.get("parameter_context"):
        lines.append(f"Parameter context: `{d['parameter_context']}`\n")

    def fmt(e: dict) -> str:
        s = f"- **{e['name']}** ({e.get('type', e['kind'])})"
        if e.get("endpoints"):
            s += ": " + ", ".join(f"{k} `{v}`" for k, v in e["endpoints"].items())
        return s
    for title, key in (("Inputs", "inputs"), ("Outputs", "outputs")):
        if d.get(key):
            lines += [f"{h}# {title}\n", *map(fmt, d[key]), ""]
    if d.get("steps"):
        lines.append(f"{h}# Steps\n")
        for i, s in enumerate(d["steps"], 1):
            lines.append(f"{i}. **{s['name']}** ({s['type']}, {s['state'].lower()})")
            for k, v in (s.get("config") or {}).items():
                lines.append(f"   - {k}: `{v}`")
            for r, dests in (s.get("routes") or {}).items():
                lines.append(f"   - {r} -> {', '.join(dests)}")
        lines.append("")
    if d.get("external_endpoints"):
        lines += [f"{h}# External endpoints\n", *(f"- `{e}`" for e in d["external_endpoints"]), ""]
    if d.get("controller_services"):
        lines += [f"{h}# Controller services\n", *map(fmt, [{**s, "kind": s["type"]} for s in d["controller_services"]]), ""]
    for g in d.get("child_groups", []):
        lines.append(to_markdown(g, level + 1))
    return "\n".join(lines)


# ---------- health_check ----------

LISTENER_PREFIXES = ("Listen", "HandleHttpRequest", "GetHTTP", "ListenHTTP")
DRIFT_STATES = ("LOCALLY_MODIFIED", "STALE", "LOCALLY_MODIFIED_AND_STALE")


async def _walk(nifi: NiFiClient, group_id: str, path: str, acc: dict) -> None:
    pgf = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]
    flow = pgf["flow"]
    invalid = [p["component"]["name"] for p in flow["processors"]
               if p["component"]["state"] == "INVALID" or p["component"].get("validationErrors")]
    invalid += [f"port '{p['component']['name']}'" for p in flow["inputPorts"] + flow["outputPorts"]
                if p["component"].get("validationErrors")]
    stopped = [p["component"]["name"] for p in flow["processors"] if p["component"]["state"] == "STOPPED"]
    if invalid or stopped:
        acc["groups"].append({"group": path, "id": group_id, **({"invalid": invalid} if invalid else {}),
                              **({"stopped": len(stopped), "stopped_names": stopped[:8]} if stopped else {})})
    for c in flow["connections"]:
        q = queue_summary(c)
        if q.get("back_pressure"):
            acc["queues"].append({"group": path, **q})
    for p in flow["processors"]:
        comp = p["component"]
        if not short_type(comp["type"]).startswith(LISTENER_PREFIXES):
            continue
        for k, v in comp["config"]["properties"].items():
            if v and "port" in k.lower() and "ssl" not in k.lower():
                acc["ports"].append({"port": v, "processor": comp["name"], "id": p["id"], "group": path,
                                     "state": comp["state"], "property": k})
    for g in flow["processGroups"]:
        vci = g["component"].get("versionControlInformation")
        if vci:
            acc["versioned"].append({"group": f"{path}/{g['component']['name']}", "id": g["id"],
                                     "flow": vci.get("flowName"), "version": vci.get("version"), "state": vci.get("state"),
                                     **({"explanation": vci["stateExplanation"]}
                                        if vci.get("state") != "UP_TO_DATE" and vci.get("stateExplanation") else {})})
        await _walk(nifi, g["id"], f"{path}/{g['component']['name']}", acc)


def _port_number(value: str, params: dict[str, str]) -> str:
    if value.startswith("#{") and value.endswith("}"):
        return params.get(value[2:-1], value)
    return value


async def health(nifi: NiFiClient, registry_port: int = 18080, limit: int = 15) -> dict:
    from urllib.parse import urlparse

    acc: dict[str, list] = {"groups": [], "queues": [], "ports": [], "versioned": []}
    await _walk(nifi, "root", "", acc)
    issues: list[str] = []

    # Version control: SYNC_FAILURE and drift.
    for v in acc["versioned"]:
        if v["state"] == "SYNC_FAILURE":
            issues.append(f"SYNC_FAILURE on {v['group']}: {v.get('explanation')}")
        elif v["state"] in DRIFT_STATES:
            issues.append(f"{v['group']} is {v['state']}")

    # Registry clients.
    registries = []
    for r in (await nifi.get("/controller/registry-clients"))["registries"]:
        rc = r["component"]
        url = next((v for k, v in (rc.get("properties") or {}).items() if "url" in k.lower() and v), None)
        entry = {"name": rc["name"], "url": url}
        try:
            buckets = (await nifi.get(f"/flow/registries/{r['id']}/buckets"))["buckets"]
            entry.update(reachable=True, buckets=len(buckets))
        except NiFiError as e:
            entry.update(reachable=False, error=str(e)[:300])
            issues.append(f"registry client {rc['name']} unreachable")
        if url and urlparse(url).port:
            registry_port = urlparse(url).port
        registries.append(entry)

    # Listening ports: duplicates, and anything on the registry's port.
    params: dict[str, str] = {}
    for c in (await nifi.get("/flow/parameter-contexts"))["parameterContexts"]:
        for p in c["component"].get("parameters", []):
            if not p["parameter"].get("sensitive") and p["parameter"].get("value"):
                params.setdefault(p["parameter"]["name"], p["parameter"]["value"])
    by_port: dict[str, list[dict]] = {}
    for p in acc["ports"]:
        p["port"] = _port_number(p["port"], params)
        by_port.setdefault(p["port"], []).append(p)
    conflicts = []
    for port, users in sorted(by_port.items()):
        if len(users) > 1:
            conflicts.append({"port": port, "reason": "used by more than one listener",
                              "processors": [f"{u['group']}/{u['processor']} ({u['state'].lower()})" for u in users]})
        if port == str(registry_port):
            conflicts.append({"port": port, "reason": f"NiFi Registry listens on {registry_port}",
                              "processors": [f"{u['group']}/{u['processor']} ({u['state'].lower()})" for u in users]})
    issues += [f"port {c['port']} conflict: {c['reason']}" for c in conflicts]

    # System diagnostics.
    sd = (await nifi.get("/system-diagnostics"))["systemDiagnostics"]["aggregateSnapshot"]
    repos = [("flowfile", sd.get("flowFileRepositoryStorageUsage"))]
    repos += [("content", r) for r in sd.get("contentRepositoryStorageUsage", [])]
    repos += [("provenance", r) for r in sd.get("provenanceRepositoryStorageUsage", [])]
    disk = [{"repository": name, "used": r.get("utilization"), "free": r.get("freeSpace"), "total": r.get("totalSpace")}
            for name, r in repos if r]
    system = {"heap_used": sd.get("usedHeap"), "heap_max": sd.get("maxHeap"), "heap_utilization": sd.get("heapUtilization"),
              "disk": disk, "uptime": sd.get("uptime")}
    heap = float(str(sd.get("heapUtilization", "0")).rstrip("%") or 0)
    if heap >= 85:
        issues.append(f"heap at {sd.get('heapUtilization')}")
    for d in disk:
        if float(str(d["used"] or "0").rstrip("%") or 0) >= 90:
            issues.append(f"{d['repository']} repository disk at {d['used']}")

    from .certs import cert_issues, check_certificates

    certificates = await check_certificates(nifi)
    issues += cert_issues(certificates)

    from .lint import design_section

    design, design_issues = await design_section(nifi, limit)
    issues += design_issues

    errors = [b for b in await bulletins(nifi, 100) if b["level"] == "ERROR"]
    bad_groups = sorted((g for g in acc["groups"] if g.get("invalid")), key=lambda g: -len(g["invalid"]))
    for g in bad_groups[:5]:
        issues.append(f"{g['group'] or '/'}: {len(g['invalid'])} invalid")
    if len(bad_groups) > 5:
        issues.append(f"{len(bad_groups) - 5} more group(s) with invalid components "
                      f"({sum(len(g['invalid']) for g in bad_groups[5:])} components)")
    if acc["queues"]:
        issues.append(f"{len(acc['queues'])} back-pressured queue(s)")
    if errors:
        issues.append(f"{len(errors)} error bulletin(s)")
    groups = sorted(acc["groups"], key=lambda g: (-len(g.get("invalid", [])), -g.get("stopped", 0)))
    for g in groups:
        if len(g.get("invalid", [])) > 8:
            g["invalid_total"], g["invalid"] = len(g["invalid"]), g["invalid"][:8]
    vc_flagged = [v for v in acc["versioned"] if v["state"] != "UP_TO_DATE"]
    truncated = {k: len(v) for k, v in (("groups", groups), ("back_pressured_queues", acc["queues"]),
                                       ("version_control", vc_flagged), ("listening_ports", acc["ports"])) if len(v) > limit}
    out = {
        "healthy": not issues, "issues": issues,
        "totals": {"groups_with_problems": len(groups), "back_pressured_queues": len(acc["queues"]),
                   "error_bulletins": len(errors), "versioned_groups": len(acc["versioned"]),
                   "up_to_date": len(acc["versioned"]) - len(vc_flagged), "listeners": len(acc["ports"])},
        "groups": groups[:limit], "back_pressured_queues": acc["queues"][:limit],
        "error_bulletins": errors[:10],
        "version_control": vc_flagged[:limit], "registries": registries,
        "listening_ports": sorted(({"port": p["port"], "processor": f"{p['group']}/{p['processor']}", "state": p["state"]}
                                   for p in acc["ports"]), key=lambda p: (p["port"], p["processor"])),
        "port_conflicts": conflicts, "system": system, "certificates": certificates, "design": design,
    }
    out["listening_ports"] = out["listening_ports"][:limit]
    if truncated:
        out["truncated"] = {"totals": truncated, "hint": "raise limit, or diagnose a group id for its details"}
    return out


def compact_description(d: dict, limit: int, offset: int, name: str | None, type_: str | None) -> dict:
    from collections import Counter

    from . import paging

    out = dict(d)
    steps = d.get("steps", [])
    if name:
        steps = [s for s in steps if name.lower() in s["name"].lower()]
    if type_:
        steps = [s for s in steps if type_.lower() in s["type"].lower()]
    hint = "describe_flow again with offset, name or type, or write markdown_file for the full description"
    out["steps"], note = paging.page(steps, limit, offset, "steps", hint)
    notes = [note] if note else []
    if len(d.get("steps", [])) > limit:
        out["step_counts"] = {"total": len(d["steps"]), "by_type": dict(Counter(s["type"] for s in d["steps"]).most_common(10))}
    cap = min(limit, 15)
    for key in ("inputs", "outputs", "external_endpoints", "controller_services"):
        if len(d.get(key, [])) > cap:
            out[key] = d[key][:cap]
            notes.append({"what": key, "total": len(d[key]), "returned": cap})
    if d.get("child_groups"):
        out["child_groups"] = [{"id": g["id"], "name": g["name"], "steps": len(g.get("steps", [])),
                                "inputs": len(g.get("inputs", [])), "outputs": len(g.get("outputs", [])),
                                "child_groups": len(g.get("child_groups", []))} for g in d["child_groups"][:limit]]
        notes.append({"what": "child_groups", "hint": "describe_flow on a child group's id for its details"})
    if notes:
        out["paging"] = notes
    return {k: v for k, v in out.items() if v not in ([], None)}
