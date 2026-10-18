"""build_flow: create a whole flow from one spec, validated against processor schemas before anything is created.

A spec is a tree of groups. Each group holds processors, funnels, input/output ports, child groups and the
connections between them; a connection end is a component of the same group ("Validate order") or a port of
a child group ("20 · Pipeline/success"). A group may instead be generated from a template (stage =
http_stage). Layouts: auto (layered), stack (pipeline of stage groups with lanes), grid (top-level groups in
rows) and manual (positions given). The parameter context is bound to every group, controller services are
created in the top group and referenced from properties as "@service:<name>".
"""

from __future__ import annotations

from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, Field

from .build import CLIENT_ID, WRITE, _delete_group, _nifi, _wait, _service_state
from .client import NiFiClient, NiFiError
from .flowplan import center, plan_grid, plan_stack, stagger_parallel
from .layout import apply_layout, free_position
from .parameters import Parameter, assign_context, create_context
from .templates import StageSpec, http_stage

COLUMN_WIDTH = 700
ROW_HEIGHT = 200
SERVICE_REF = "@service:"


class ProcessorSpec(BaseModel):
    name: str = Field(description="Unique within its group; connections and sections refer to it")
    type: str = Field(description="Fully qualified class, e.g. org.apache.nifi.processors.standard.SplitJson")
    properties: dict[str, str | None] = Field(default_factory=dict, description=(
        "Property names, not display names. Reference a controller service of this flow as '@service:<name>'"))
    auto_terminate: list[str] = Field(default_factory=list)
    scheduling_period: str | None = None
    penalty: str | None = Field(None, description="Penalty duration, e.g. '5 sec' (InvokeHTTP's default is 30 sec)")
    comments: str | None = Field(None, description="Say here why a failure relationship may be auto-terminated")
    position: tuple[float, float] | None = Field(None, description="Used by the manual layout")


class ConnectionSpec(BaseModel):
    source: str = Field(description="Component of this group, or '<child group>/<output port>'")
    destination: str = Field(description="Component of this group, or '<child group>/<input port>'")
    relationships: list[str] = Field(default_factory=list, description="Required when the source is a processor")
    name: str | None = None
    bends: list[tuple[float, float]] | None = Field(None, description="Explicit bend points; overrides the layout's")
    back_pressure_count: int | None = Field(None, description="Back pressure object threshold (NiFi default 10,000)")


class SectionSpec(BaseModel):
    title: str = Field(description="Header label text")
    members: list[str] = Field(description="Processor and funnel names under this header")


class ParameterContextSpec(BaseModel):
    name: str
    description: str | None = None
    parameters: list[Parameter] = Field(default_factory=list)


class ServiceSpec(BaseModel):
    name: str = Field(description="Referenced from processor properties as '@service:<name>'")
    type: str = Field(description="Fully qualified class, e.g. org.apache.nifi.http.StandardHttpContextMap")
    properties: dict[str, str | None] = Field(default_factory=dict)


class GroupSpec(BaseModel):
    name: str = Field(description="Unique among its siblings; must not contain '/'")
    processors: list[ProcessorSpec] = Field(default_factory=list)
    connections: list[ConnectionSpec] = Field(default_factory=list)
    funnels: list[str] = Field(default_factory=list)
    input_ports: list[str] = Field(default_factory=list)
    output_ports: list[str] = Field(default_factory=list)
    groups: list[GroupSpec] = Field(default_factory=list)
    stage: StageSpec | None = Field(None, description="Generate this group from a template (processors, ports and connections)")
    layout: Literal["auto", "stack", "grid", "manual"] = "auto"
    columns: int = Field(3, description="Groups per row for the grid layout")
    positions: dict[str, tuple[float, float]] = Field(default_factory=dict, description="Manual layout: name -> (x, y)")
    sections: list[SectionSpec] = Field(default_factory=list, description="Auto layout: header labels over members")
    comments: str | None = None


GroupSpec.model_rebuild()


def expand(g: GroupSpec, errors: list[str]) -> GroupSpec:
    """Apply templates, depth first. A template group must not define its own contents (reported in errors)."""
    if g.stage is not None:
        if g.processors or g.connections or g.funnels or g.input_ports or g.output_ports or g.groups:
            errors.append(f"group {g.name!r}: a stage template group cannot also define processors, ports, "
                          "connections or groups")
            return g.model_copy(update={"stage": None, "groups": []})
        t = http_stage(g.stage)
        return g.model_copy(update={
            "processors": [ProcessorSpec(**p) for p in t["processors"]],
            "connections": [ConnectionSpec(**c) for c in t["connections"]],
            "input_ports": t["input_ports"], "output_ports": t["output_ports"],
            "positions": t["positions"], "layout": "manual", "stage": g.stage})
    return g.model_copy(update={"groups": [expand(c, errors) for c in g.groups]})


def walk(g: GroupSpec, path: str = ""):
    here = f"{path}/{g.name}" if path else g.name
    yield here, g
    for c in g.groups:
        yield from walk(c, here)


async def _definitions(nifi: NiFiClient, types: set[str]) -> tuple[dict[str, dict], dict[str, dict], list[str]]:
    catalog = {t["type"]: t["bundle"] for t in (await nifi.get("/flow/processor-types"))["processorTypes"]}
    bundles, defs, errors = {}, {}, []
    for t in sorted(types):
        b = catalog.get(t)
        if b is None:
            errors.append(f"unknown processor type {t!r}")
            continue
        bundles[t] = b
        defs[t] = await nifi.get(f"/flow/processor-definition/{b['group']}/{b['artifact']}/{b['version']}/{t}")
    return bundles, defs, errors


def _validate(processors: list[ProcessorSpec], funnels: list[str], connections: list[ConnectionSpec],
              sections: list[SectionSpec], defs: dict[str, dict], g: GroupSpec | None = None,
              services: set[str] | None = None, where: str = "") -> list[str]:
    errors: list[str] = []
    at = f"{where}: " if where else ""
    inputs, outputs = (g.input_ports, g.output_ports) if g else ([], [])
    children = {c.name: c for c in g.groups} if g else {}
    names = [p.name for p in processors] + funnels + inputs + outputs
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        errors.append(f"{at}duplicate component names: {dupes}")
    child_names = [c.name for c in g.groups] if g else []
    if len(set(child_names)) != len(child_names):
        errors.append(f"{at}duplicate child group names")
    for n in child_names:
        if "/" in n:
            errors.append(f"{at}child group name {n!r} must not contain '/'")
    procs = {p.name: p for p in processors}
    for p in processors:
        d = defs.get(p.type)
        if services is not None:
            for prop, v in p.properties.items():
                if v and v.startswith(SERVICE_REF) and v[len(SERVICE_REF):] not in services:
                    errors.append(f"{at}{p.name}: property {prop!r} references unknown service {v[len(SERVICE_REF):]!r}")
        if d is None:
            continue
        descriptors = d.get("propertyDescriptors") or {}
        by_display = {v.get("displayName"): k for k, v in descriptors.items()}
        rels = {r["name"] for r in d.get("supportedRelationships", [])}
        for prop in p.properties:
            if prop in descriptors:
                continue
            if prop in by_display:
                errors.append(f"{at}{p.name}: property {prop!r} is a display name; use {by_display[prop]!r}")
            elif not d.get("supportsDynamicProperties"):
                errors.append(f"{at}{p.name}: unknown property {prop!r}")
        if not d.get("supportsDynamicRelationships"):
            bad = [r for r in p.auto_terminate if r not in rels]
            if bad:
                errors.append(f"{at}{p.name}: unknown auto_terminate relationships {bad} (has {sorted(rels)})")

    def end_ok(end: str, as_source: bool) -> str | None:
        if end not in names and "/" in end:
            child, port = split_end(end, children)
            if child not in children:
                return f"no component or child group port named {end!r}"
            ports = children[child].output_ports if as_source else children[child].input_ports
            if port not in ports:
                return f"child group {child!r} has no {'output' if as_source else 'input'} port {port!r}"
            return None
        if end not in names:
            return f"no component named {end!r}"
        if as_source and end in outputs:
            return f"{end!r} is an output port of this group; it can only be a destination"
        if not as_source and end in inputs:
            return f"{end!r} is an input port of this group; it can only be a source"
        return None

    for c in connections:
        for end, as_source in ((c.source, True), (c.destination, False)):
            problem = end_ok(end, as_source)
            if problem:
                errors.append(f"{at}connection {c.source} -> {c.destination}: {problem}")
        src = procs.get(c.source)
        if src is None:
            if c.relationships:
                errors.append(f"{at}connection {c.source} -> {c.destination}: relationships only apply to a processor source")
            continue
        if not c.relationships:
            errors.append(f"{at}connection {c.source} -> {c.destination}: relationships required for a processor source")
        d = defs.get(src.type)
        if d and not d.get("supportsDynamicRelationships"):
            rels = {r["name"] for r in d.get("supportedRelationships", [])}
            bad = [r for r in c.relationships if r not in rels]
            if bad:
                errors.append(f"{at}connection {c.source} -> {c.destination}: unknown relationships {bad} (has {sorted(rels)})")
    for s in sections:
        bad = [m for m in s.members if m not in names]
        if bad:
            errors.append(f"{at}section {s.title!r}: no components named {bad}")
    if g and g.layout == "stack" and not g.groups:
        errors.append(f"{at}layout 'stack' needs child groups to stack")
    return errors


def split_end(end: str, children) -> tuple[str, str]:
    """'<child group>/<port>' -> (child, port); the child is the longest child-group name that prefixes end."""
    for child in sorted(children, key=len, reverse=True):
        if end.startswith(child + "/"):
            return child, end[len(child) + 1:]
    return end.split("/", 1)[0], end.split("/", 1)[1]


def _drawn(end: str, g: GroupSpec, items: dict[str, str]) -> str:
    """The component a connection end is drawn as on its group's canvas (a child's port -> the child)."""
    if end in items:
        return end
    return split_end(end, [c.name for c in g.groups])[0]


def _items(g: GroupSpec) -> dict[str, str]:
    items = {p.name: "processor" for p in g.processors}
    items |= {f: "funnel" for f in g.funnels}
    items |= {p: "input_port" for p in g.input_ports}
    items |= {p: "output_port" for p in g.output_ports}
    items |= {c.name: "process_group" for c in g.groups}
    return items


def _is_side(c: ConnectionSpec, g: GroupSpec, items: dict[str, str]) -> bool:
    """Same rule as layout._is_side: named relationships or a child's output port, none of them success."""
    names = list(c.relationships)
    if c.source not in items:
        names.append(split_end(c.source, [k.name for k in g.groups])[1])
    return bool(names) and "success" not in names


def _plan(g: GroupSpec) -> tuple[dict[str, tuple[float, float]], dict[int, list[tuple[float, float]]], dict[int, int]]:
    items = _items(g)
    conns = [{"idx": i, "src": _drawn(c.source, g, items), "dst": _drawn(c.destination, g, items),
              "side": _is_side(c, g, items)} for i, c in enumerate(g.connections)]
    labels: dict[int, int] = {}
    if g.layout == "stack":
        positions, bends, labels = plan_stack(items, [c.name for c in g.groups], g.input_ports, g.output_ports, conns)
    elif g.layout == "grid":
        positions, bends, labels = plan_grid(items, [c.name for c in g.groups], conns, g.columns)
    else:
        positions, bends = {}, {}
        # Seed positions: one column per section with its header on top, so the auto layout keeps them apart.
        column = {m: i for i, s in enumerate(g.sections) for m in s.members}
        rows: dict[int, int] = {}
        for n in items:
            if n in g.positions:
                positions[n] = g.positions[n]
                continue
            p = next((p for p in g.processors if p.name == n), None)
            if p and p.position:
                positions[n] = p.position
                continue
            col = column.get(n, len(g.sections))
            rows[col] = rows.get(col, 0) + 1
            positions[n] = (col * COLUMN_WIDTH, rows[col] * ROW_HEIGHT)
    for i, c in enumerate(g.connections):
        if c.bends is not None:
            bends[i] = list(c.bends)
            labels.pop(i, None)
    if g.layout == "manual":
        # A retry loop (A -> B and B -> A) or parallel connections would draw as one line: spread them.
        centers = {n: center(positions[n], kind) for n, kind in items.items()}
        stagger_parallel(conns, centers, bends)
    return positions, bends, labels


async def _create_group(nifi: NiFiClient, parent_id: str, g: GroupSpec, pos: tuple[float, float],
                        context_id: str | None, bundles: dict[str, dict], services: dict[str, str]) -> dict:
    rev = {"clientId": CLIENT_ID, "version": 0}
    component: dict[str, Any] = {"name": g.name, "position": {"x": pos[0], "y": pos[1]}}
    if g.comments:
        component["comments"] = g.comments
    gid = (await nifi.post(f"/process-groups/{parent_id}/process-groups", {"revision": rev, "component": component}))["id"]
    if context_id:
        # A group created through the API does not inherit its parent's context: bind every group.
        await assign_context(nifi, gid, context_id)
    return await _create_contents(nifi, gid, g, context_id, bundles, services)


async def _create_contents(nifi: NiFiClient, gid: str, g: GroupSpec, context_id: str | None,
                           bundles: dict[str, dict], services: dict[str, str]) -> dict:
    """Create g's processors, funnels, ports, labels, child groups and connections inside group gid."""
    rev = {"clientId": CLIENT_ID, "version": 0}
    positions, bends, labels = _plan(g)

    def xy(name: str) -> dict:
        x, y = positions.get(name, (0, 0))
        return {"x": round(x), "y": round(y)}

    ids: dict[str, tuple[str, str, str]] = {}
    for p in g.processors:
        props = {k: (services[v[len(SERVICE_REF):]] if v and v.startswith(SERVICE_REF) else v) for k, v in p.properties.items()}
        config: dict[str, Any] = {"properties": props, "autoTerminatedRelationships": p.auto_terminate}
        if p.scheduling_period:
            config["schedulingPeriod"] = p.scheduling_period
        if p.penalty:
            config["penaltyDuration"] = p.penalty
        if p.comments:
            config["comments"] = p.comments
        r = await nifi.post(f"/process-groups/{gid}/processors", {"revision": rev, "component": {
            "type": p.type, "bundle": bundles[p.type], "name": p.name, "position": xy(p.name), "config": config}})
        ids[p.name] = (r["id"], "PROCESSOR", gid)
    for f in g.funnels:
        r = await nifi.post(f"/process-groups/{gid}/funnels", {"revision": rev, "component": {"position": xy(f)}})
        ids[f] = (r["id"], "FUNNEL", gid)
    for kind, path, names in (("INPUT_PORT", "input-ports", g.input_ports), ("OUTPUT_PORT", "output-ports", g.output_ports)):
        for n in names:
            r = await nifi.post(f"/process-groups/{gid}/{path}", {"revision": rev, "component": {"name": n, "position": xy(n)}})
            ids[n] = (r["id"], kind, gid)
    for i, s in enumerate(g.sections):
        await nifi.post(f"/process-groups/{gid}/labels", {"revision": rev, "component": {
            "label": s.title, "position": {"x": i * COLUMN_WIDTH - 20, "y": 0}, "width": 480, "height": 40,
            "style": {"font-size": "14px"}}})
    children = {}
    for c in g.groups:
        x, y = positions.get(c.name, (0, 0))
        children[c.name] = await _create_group(nifi, gid, c, (x, y), context_id, bundles, services)

    def resolve(end: str) -> tuple[str, str, str]:
        if end in ids:
            return ids[end]
        child, port = split_end(end, children)
        return children[child]["_ports"][port]

    conn_ids = []
    for i, c in enumerate(g.connections):
        (sid, stype, sg), (did, dtype, dg) = resolve(c.source), resolve(c.destination)
        comp: dict[str, Any] = {"source": {"id": sid, "groupId": sg, "type": stype},
                                "destination": {"id": did, "groupId": dg, "type": dtype}}
        if c.relationships:
            comp["selectedRelationships"] = c.relationships
        if c.name:
            comp["name"] = c.name
        if bends.get(i):
            comp["bends"] = [{"x": round(x), "y": round(y)} for x, y in bends[i]]
            comp["labelIndex"] = labels.get(i, 0)
        if c.back_pressure_count:
            comp["backPressureObjectThreshold"] = c.back_pressure_count
        r = await nifi.post(f"/process-groups/{gid}/connections", {"revision": rev, "component": comp})
        conn_ids.append({"id": r["id"], "source": c.source, "destination": c.destination, "relationships": c.relationships})
    layout = await apply_layout(nifi, gid) if g.layout == "auto" else {"group_id": gid, "layout": g.layout}
    return {"group_id": gid, "name": g.name,
            "processors": {n: v[0] for n, v in ids.items() if v[1] == "PROCESSOR"},
            "funnels": {n: v[0] for n, v in ids.items() if v[1] == "FUNNEL"},
            "input_ports": {n: ids[n][0] for n in g.input_ports},
            "output_ports": {n: ids[n][0] for n in g.output_ports},
            "connections": conn_ids, "layout": layout,
            "groups": {n: {k: v for k, v in r.items() if not k.startswith("_")} for n, r in children.items()},
            "_ports": {n: ids[n] for n in g.input_ports + g.output_ports}}


async def _validation_errors(nifi: NiFiClient, result: dict, path: str = "") -> dict[str, list[str]]:
    invalid = {}
    for n, pid in result["processors"].items():
        errs = (await nifi.get(f"/processors/{pid}"))["component"].get("validationErrors")
        if errs:
            invalid[f"{path}{n}"] = errs
    for cname, child in result["groups"].items():
        invalid |= await _validation_errors(nifi, child, f"{path}{cname}/")
    return invalid


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def build_flow(
        ctx: Context, name: str, processors: list[ProcessorSpec] | None = None,
        connections: list[ConnectionSpec] | None = None,
        funnels: list[str] | None = None, sections: list[SectionSpec] | None = None,
        parameter_context: ParameterContextSpec | None = None, parameter_context_id: str | None = None,
        parent_id: str = "root", input_ports: list[str] | None = None, output_ports: list[str] | None = None,
        groups: list[GroupSpec] | None = None, controller_services: list[ServiceSpec] | None = None,
        layout: Literal["auto", "stack", "grid", "manual"] = "auto", columns: int = 3,
        positions: dict[str, tuple[float, float]] | None = None, start: bool = False, dry_run: bool = False,
    ) -> dict:
        """Build a whole flow in one call inside a new process group, nested groups included.

        The new group holds processors, funnels, ports and child groups (`groups`, each a GroupSpec with the
        same fields, nested as deep as needed). A connection end is a component of the same group or
        '<child group>/<port>'. A child group with `stage` is generated from the http_stage template: the
        stage contract (ports in/success/rejected/failure/ledger), HTTP retries and the rejected-vs-failure
        decision (see get_design_guide). Layouts: auto (layered), stack (stage groups top to bottom with
        lanes for rejected/failure/ledger), grid (groups in rows of `columns`, ~1,000 apart), manual
        (`positions`). Connections may carry explicit `bends`, a `name` and a back pressure count.

        Every property, relationship and connection end is checked before anything is created; problems
        come back together as one list. parameter_context creates a new context, parameter_context_id binds
        an existing one; either is bound to every group. controller_services are created in the new group,
        referenced from properties as '@service:<name>' and enabled. start=true also starts the flow.
        If creation fails partway, the group (and a context it created) is removed. The result includes
        check_flow_design's warnings for the new group. dry_run=true only validates the spec (nothing is
        created) and returns its size, so a caller can check a spec before replacing a running flow.
        """
        from .lint import lint_group, summarize

        nifi = _nifi(ctx)
        if parameter_context and parameter_context_id:
            raise ToolError("Pass parameter_context or parameter_context_id, not both")
        errors: list[str] = []
        top = expand(GroupSpec(name=name, processors=processors or [], connections=connections or [],
                               funnels=funnels or [], sections=sections or [], input_ports=input_ports or [],
                               output_ports=output_ports or [], groups=groups or [], layout=layout,
                               columns=columns, positions=positions or {}), errors)
        service_specs = controller_services or []
        types = {p.type for _, g in walk(top) for p in g.processors}
        bundles, defs, type_errors = await _definitions(nifi, types)
        errors += type_errors
        svc_catalog = {t["type"]: t["bundle"] for t in (await nifi.get("/flow/controller-service-types"))["controllerServiceTypes"]}
        errors += [f"unknown controller service type {s.type!r}" for s in service_specs if s.type not in svc_catalog]
        svc_names = {s.name for s in service_specs}
        for path, g in walk(top):
            errors += _validate(g.processors, g.funnels, g.connections, g.sections, defs, g, svc_names,
                                path if g is not top else "")
        if errors:
            raise ToolError("Flow spec is invalid; nothing was created:\n- " + "\n- ".join(errors))
        if dry_run:
            tree = list(walk(top))
            return {"valid": True, "groups": len(tree), "processors": sum(len(g.processors) for _, g in tree),
                    "connections": sum(len(g.connections) for _, g in tree), "controller_services": len(service_specs)}

        x, y = await free_position(nifi, parent_id, "process_group")
        context = None
        gid = None
        try:
            if parameter_context:
                context = await create_context(nifi, parameter_context.name, parameter_context.parameters,
                                               parameter_context.description)
            context_id = context["id"] if context else parameter_context_id
            # Services first, in a holder group, so processors can reference them by id; they move with it.
            rev = {"clientId": CLIENT_ID, "version": 0}
            holder = (await nifi.post(f"/process-groups/{parent_id}/process-groups",
                                      {"revision": rev, "component": {"name": name, "position": {"x": x, "y": y}}}))
            gid = holder["id"]
            if context_id:
                await assign_context(nifi, gid, context_id)
            services: dict[str, str] = {}
            for s in service_specs:
                r = await nifi.post(f"/process-groups/{gid}/controller-services", {"revision": rev, "component": {
                    "type": s.type, "bundle": svc_catalog[s.type], "name": s.name, "properties": s.properties}})
                services[s.name] = r["id"]
            result = await _create_contents(nifi, gid, top, context_id, bundles, services)
            for sid in services.values():
                cur = await nifi.get(f"/controller-services/{sid}")
                await nifi.request("PUT", f"/controller-services/{sid}/run-status",
                                   json={"revision": {"clientId": CLIENT_ID, "version": cur["revision"]["version"]},
                                         "state": "ENABLED"})
            for sid in services.values():
                await _wait(lambda sid=sid: _service_state(nifi, sid, "ENABLED"))
        except Exception:
            if gid:
                await _delete_group(nifi, gid)
            if context:
                fresh = await nifi.get(f"/parameter-contexts/{context['id']}")
                await nifi.request("DELETE", f"/parameter-contexts/{context['id']}",
                                   params={"version": fresh["revision"]["version"], "clientId": CLIENT_ID})
            raise

        invalid = await _validation_errors(nifi, result)
        started = False
        if start:
            try:
                await nifi.request("PUT", f"/flow/process-groups/{gid}", json={"id": gid, "state": "RUNNING"})
                started = True
            except NiFiError as e:
                invalid["(start)"] = [str(e)[:300]]
        design = summarize(await lint_group(nifi, gid, True, name), 20)
        return {"group_id": gid, "name": name,
                "parameter_context_id": context["id"] if context else parameter_context_id,
                "controller_services": services, **{k: result[k] for k in (
                    "processors", "funnels", "input_ports", "output_ports", "connections", "layout", "groups")},
                "validation_errors": invalid or None, "design": design, "started": started,
                "next": ("Fix any validation_errors and design warnings, then set_run_state the group RUNNING "
                         "(or pass start=true) and test with inject_test_flowfile.")}

