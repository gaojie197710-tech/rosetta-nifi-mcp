"""Phase 2 tools: build, run and test flows. Registered only when NIFI_MCP_ALLOW_WRITES is true."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations

from mcp.server.mcpserver.exceptions import ToolError

from .client import NiFiClient, NiFiError

WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=False)
CLIENT_ID = f"nifi-mcp-{uuid.uuid4()}"
INJECTOR_PREFIX = "[mcp-test] inject -> "
GENERATE_FLOWFILE = "org.apache.nifi.processors.standard.GenerateFlowFile"

# NiFi REST collection per component kind, and the connectable type a connection uses for it.
KIND_PATH = {"processor": "processors", "input_port": "input-ports", "output_port": "output-ports", "funnel": "funnels"}
CONNECTABLE = {"processor": "PROCESSOR", "input_port": "INPUT_PORT", "output_port": "OUTPUT_PORT", "funnel": "FUNNEL"}


def _nifi(ctx: Context) -> NiFiClient:
    return ctx.request_context.lifespan_context


def _rev(entity: dict) -> dict:
    return {"clientId": CLIENT_ID, "version": entity["revision"]["version"]}


async def _bundle_for(nifi: NiFiClient, list_path: str, key: str, type_: str) -> dict:
    types = (await nifi.get(list_path))[key]
    match = next((t for t in types if t["type"] == type_), None)
    if match is None:
        raise ToolError(f"Unknown type {type_!r}; search for it first")
    return match["bundle"]


async def _find_connectable(nifi: NiFiClient, component_id: str) -> tuple[str, dict]:
    for kind, path in KIND_PATH.items():
        try:
            return kind, await nifi.get(f"/{path}/{component_id}")
        except NiFiError as e:
            if "404" not in str(e):
                raise
    raise ToolError(f"No processor, port or funnel with id {component_id}")


async def _wait(predicate, timeout: float = 15, interval: float = 0.5) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _processor_summary(p: dict) -> dict:
    c = p["component"]
    return {"id": p["id"], "name": c["name"], "type": c["type"], "state": c["state"],
            "properties": c["config"]["properties"], "auto_terminated": c["config"].get("autoTerminatedRelationships"),
            "relationships": [r["name"] for r in c.get("relationships", [])],
            "validation_errors": c.get("validationErrors")}


async def _drop_queue(nifi: NiFiClient, connection_id: str) -> str:
    drop = await nifi.run_async_request(f"/flowfile-queues/{connection_id}/drop-requests", None, "dropRequest")
    return (drop.get("dropped") or "0").split(" ")[0]


def register(mcp: MCPServer) -> None:
    from .layout import free_position

    # ---------- build ----------

    @mcp.tool(annotations=WRITE)
    async def create_process_group(ctx: Context, name: str, parent_id: str = "root", x: float | None = None, y: float | None = None) -> dict:
        """Create an empty process group. Build and test new flows inside their own group.
        Without x/y it is placed in free space below the existing components."""
        if x is None or y is None:
            x, y = await free_position(_nifi(ctx), parent_id, "process_group")
        body = {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": {"name": name, "position": {"x": x, "y": y}}}
        g = await _nifi(ctx).post(f"/process-groups/{parent_id}/process-groups", body)
        return {"id": g["id"], "name": g["component"]["name"], "parent_id": parent_id}

    @mcp.tool(annotations=WRITE)
    async def create_processor(
        ctx: Context, group_id: str, processor_type: str, name: str | None = None,
        properties: dict[str, str | None] | None = None, auto_terminate: list[str] | None = None,
        scheduling_period: str | None = None, below_id: str | None = None,
        x: float | None = None, y: float | None = None,
    ) -> dict:
        """Add a processor to a group. Call get_processor_schema first for exact property names.

        properties keys are property names (not display names). auto_terminate lists relationships to drop.
        Placement: pass below_id (usually the upstream component) to put it directly underneath, or x/y;
        otherwise it goes in free space below everything else. layout_process_group tidies a whole group.
        """
        nifi = _nifi(ctx)
        if x is None or y is None:
            x, y = await free_position(nifi, group_id, "processor", below_id)
        bundle = await _bundle_for(nifi, "/flow/processor-types", "processorTypes", processor_type)
        config: dict[str, Any] = {}
        if properties:
            config["properties"] = properties
        if auto_terminate:
            config["autoTerminatedRelationships"] = auto_terminate
        if scheduling_period:
            config["schedulingPeriod"] = scheduling_period
        component = {"type": processor_type, "bundle": bundle, "position": {"x": x, "y": y}, "config": config}
        if name:
            component["name"] = name
        p = await nifi.post(f"/process-groups/{group_id}/processors", {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": component})
        return _processor_summary(p)

    @mcp.tool(annotations=WRITE)
    async def update_processor(
        ctx: Context, processor_id: str, properties: dict[str, str | None] | None = None,
        auto_terminate: list[str] | None = None, scheduling_period: str | None = None, name: str | None = None,
    ) -> dict:
        """Change a stopped processor's properties (null value removes one), auto-terminated relationships, schedule or name."""
        nifi = _nifi(ctx)
        current = await nifi.get(f"/processors/{processor_id}")
        if current["component"]["state"] == "RUNNING":
            raise ToolError("Processor is running; stop it with set_run_state first")
        config: dict[str, Any] = {}
        if properties is not None:
            config["properties"] = properties
        if auto_terminate is not None:
            config["autoTerminatedRelationships"] = auto_terminate
        if scheduling_period:
            config["schedulingPeriod"] = scheduling_period
        component: dict[str, Any] = {"id": processor_id, "config": config}
        if name:
            component["name"] = name
        p = await nifi.request("PUT", f"/processors/{processor_id}", json={"revision": _rev(current), "component": component})
        return _processor_summary(p.json())

    @mcp.tool(annotations=WRITE)
    async def connect(
        ctx: Context, source_id: str, destination_id: str, relationships: list[str] | None = None,
        group_id: str | None = None, back_pressure_count: int | None = None,
    ) -> dict:
        """Connect two components (processors, ports or funnels) in the same group.

        relationships is required when the source is a processor (e.g. ["success"]).
        """
        nifi = _nifi(ctx)
        s_kind, src = await _find_connectable(nifi, source_id)
        d_kind, dst = await _find_connectable(nifi, destination_id)
        gid = group_id or src["component"]["parentGroupId"]
        component: dict[str, Any] = {
            "source": {"id": source_id, "groupId": src["component"]["parentGroupId"], "type": CONNECTABLE[s_kind]},
            "destination": {"id": destination_id, "groupId": dst["component"]["parentGroupId"], "type": CONNECTABLE[d_kind]},
        }
        if s_kind == "processor":
            if not relationships:
                raise ToolError("relationships is required when the source is a processor")
            component["selectedRelationships"] = relationships
        if back_pressure_count:
            component["backPressureObjectThreshold"] = back_pressure_count
        c = await nifi.post(f"/process-groups/{gid}/connections", {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": component})
        return {"id": c["id"], "source": src["component"].get("name", s_kind), "destination": dst["component"].get("name", d_kind),
                "relationships": c["component"].get("selectedRelationships")}

    @mcp.tool(annotations=WRITE)
    async def create_funnel(ctx: Context, group_id: str, below_id: str | None = None,
                            x: float | None = None, y: float | None = None) -> dict:
        """Add a funnel: a cheap sink for collecting test output in a queue you can inspect.
        Pass below_id (the component feeding it) to place it underneath."""
        if x is None or y is None:
            x, y = await free_position(_nifi(ctx), group_id, "funnel", below_id)
        f = await _nifi(ctx).post(f"/process-groups/{group_id}/funnels",
                                  {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": {"position": {"x": x, "y": y}}})
        return {"id": f["id"], "group_id": group_id}

    @mcp.tool(annotations=WRITE)
    async def create_controller_service(
        ctx: Context, group_id: str, service_type: str, name: str | None = None, properties: dict[str, str | None] | None = None,
    ) -> dict:
        """Add a controller service (record readers/writers, connection pools...) to a group. It starts disabled."""
        nifi = _nifi(ctx)
        bundle = await _bundle_for(nifi, "/flow/controller-service-types", "controllerServiceTypes", service_type)
        component: dict[str, Any] = {"type": service_type, "bundle": bundle, "properties": properties or {}}
        if name:
            component["name"] = name
        s = await nifi.post(f"/process-groups/{group_id}/controller-services", {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": component})
        return {"id": s["id"], "name": s["component"]["name"], "state": s["component"]["state"],
                "validation_errors": s["component"].get("validationErrors")}

    @mcp.tool(annotations=WRITE)
    async def update_controller_service(ctx: Context, service_id: str, properties: dict[str, str | None]) -> dict:
        """Change a disabled controller service's properties."""
        nifi = _nifi(ctx)
        current = await nifi.get(f"/controller-services/{service_id}")
        s = (await nifi.request("PUT", f"/controller-services/{service_id}",
                                json={"revision": _rev(current), "component": {"id": service_id, "properties": properties}})).json()
        return {"id": s["id"], "state": s["component"]["state"], "properties": s["component"]["properties"],
                "validation_errors": s["component"].get("validationErrors")}

    # ---------- run ----------

    @mcp.tool(annotations=WRITE)
    async def set_run_state(
        ctx: Context, component_id: str,
        state: Literal["RUNNING", "STOPPED", "RUN_ONCE", "DISABLED", "ENABLED"],
        kind: Literal["processor", "process_group", "controller_service"] = "processor",
    ) -> dict:
        """Start/stop a processor (RUN_ONCE triggers it a single time), a whole process group,
        or enable/disable a controller service. Waits until the new state takes effect."""
        nifi = _nifi(ctx)
        if kind == "process_group":
            if state not in ("RUNNING", "STOPPED"):
                raise ToolError("Process groups take RUNNING or STOPPED")
            await nifi.request("PUT", f"/flow/process-groups/{component_id}", json={"id": component_id, "state": state})
            if state == "STOPPED":
                async def stopped() -> bool:
                    s = (await nifi.get(f"/flow/process-groups/{component_id}/status", recursive="true"))["processGroupStatus"]["aggregateSnapshot"]
                    return s.get("activeThreadCount", 0) == 0
                await _wait(stopped)
            return {"id": component_id, "state": state}
        if kind == "controller_service":
            if state not in ("ENABLED", "DISABLED"):
                raise ToolError("Controller services take ENABLED or DISABLED")
            current = await nifi.get(f"/controller-services/{component_id}")
            await nifi.request("PUT", f"/controller-services/{component_id}/run-status", json={"revision": _rev(current), "state": state})

            async def settled() -> bool:
                return (await nifi.get(f"/controller-services/{component_id}"))["component"]["state"] == state
            ok = await _wait(settled)
            s = (await nifi.get(f"/controller-services/{component_id}"))["component"]
            return {"id": component_id, "state": s["state"], "reached": ok, "validation_errors": s.get("validationErrors")}
        current = await nifi.get(f"/processors/{component_id}")
        await nifi.request("PUT", f"/processors/{component_id}/run-status", json={"revision": _rev(current), "state": state})
        target = "STOPPED" if state == "RUN_ONCE" else state

        async def reached() -> bool:
            p = await nifi.get(f"/processors/{component_id}")
            return p["component"]["state"] == target and (target != "STOPPED" or p["status"]["aggregateSnapshot"].get("activeThreadCount", 0) == 0)
        ok = await _wait(reached)
        p = (await nifi.get(f"/processors/{component_id}"))["component"]
        return {"id": component_id, "state": p["state"], "reached": ok, "validation_errors": p.get("validationErrors")}

    # ---------- test loop ----------

    @mcp.tool(annotations=WRITE)
    async def inject_test_flowfile(
        ctx: Context, destination_id: str, content: str, attributes: dict[str, str] | None = None, count: int = 1,
    ) -> dict:
        """Send test FlowFile(s) with the given content and attributes into a processor, port or funnel.

        Uses a stopped GenerateFlowFile named '[mcp-test] inject -> <destination>' next to the destination,
        created on first use and reused afterwards; each call triggers it once per FlowFile.
        """
        return await inject(_nifi(ctx), destination_id, content, attributes, count)

    @mcp.tool(annotations=DESTRUCTIVE)
    async def empty_queue(ctx: Context, connection_id: str) -> dict:
        """Drop every FlowFile queued in a connection."""
        dropped = await _drop_queue(_nifi(ctx), connection_id)
        return {"connection_id": connection_id, "dropped": dropped}

    @mcp.tool(annotations=DESTRUCTIVE)
    async def delete_component(
        ctx: Context, component_id: str,
        kind: Literal["processor", "connection", "funnel", "input_port", "output_port", "controller_service", "process_group"],
    ) -> dict:
        """Delete a component. Processors are stopped and their connections removed first; a process group is
        stopped, its queues emptied and its controller services disabled before it is deleted with everything in it."""
        nifi = _nifi(ctx)
        path = {"connection": "connections", "controller_service": "controller-services", "process_group": "process-groups", **KIND_PATH}[kind]
        if kind == "process_group":
            await _prepare_group_delete(nifi, component_id)
        elif kind == "processor":
            p = await nifi.get(f"/processors/{component_id}")
            if p["component"]["state"] == "RUNNING":
                await nifi.request("PUT", f"/processors/{component_id}/run-status", json={"revision": _rev(p), "state": "STOPPED"})
                await _wait(lambda: _processor_idle(nifi, component_id))
            gid = p["component"]["parentGroupId"]
            conns = (await nifi.get(f"/process-groups/{gid}/connections"))["connections"]
            for c in conns:
                if component_id in (c["component"]["source"]["id"], c["component"]["destination"]["id"]):
                    await _drop_queue(nifi, c["id"])
                    fresh = await nifi.get(f"/connections/{c['id']}")
                    await nifi.request("DELETE", f"/connections/{c['id']}", params={"version": fresh["revision"]["version"], "clientId": CLIENT_ID})
        elif kind == "controller_service":
            s = await nifi.get(f"/controller-services/{component_id}")
            if s["component"]["state"] != "DISABLED":
                await nifi.request("PUT", f"/controller-services/{component_id}/run-status", json={"revision": _rev(s), "state": "DISABLED"})
                await _wait(lambda: _service_state(nifi, component_id, "DISABLED"))
        elif kind == "connection":
            await _drop_queue(nifi, component_id)
        current = await nifi.get(f"/{path}/{component_id}")
        await nifi.request("DELETE", f"/{path}/{component_id}", params={"version": current["revision"]["version"], "clientId": CLIENT_ID})
        return {"deleted": component_id, "kind": kind}


async def inject(nifi: NiFiClient, destination_id: str, content: str, attributes: dict[str, str] | None = None, count: int = 1) -> dict:
    d_kind, dst = await _find_connectable(nifi, destination_id)
    gid = dst["component"]["parentGroupId"]
    name = f"{INJECTOR_PREFIX}{dst['component'].get('name') or destination_id}"
    flow = (await nifi.get(f"/flow/process-groups/{gid}"))["processGroupFlow"]["flow"]
    gen = next((p for p in flow["processors"] if p["component"]["name"] == name), None)
    props: dict[str, str | None] = {"Custom Text": content, "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"}
    if gen:
        # Drop attributes left over from the previous injection.
        old = gen["component"]["config"]["properties"]
        builtin = {"File Size", "Batch Size", "Data Format", "Unique FlowFiles", "Custom Text", "Character Set", "Mime Type"}
        props.update({k: None for k in old if k not in builtin})
    props.update(attributes or {})
    if gen is None:
        pos = dst.get("position") or dst["component"].get("position") or {"x": 0, "y": 0}
        ix, iy = pos["x"] - 450, pos["y"]
        bundle = await _bundle_for(nifi, "/flow/processor-types", "processorTypes", GENERATE_FLOWFILE)
        gen = await nifi.post(f"/process-groups/{gid}/processors", {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {"type": GENERATE_FLOWFILE, "bundle": bundle, "name": name,
                          "position": {"x": ix, "y": iy},
                          "config": {"properties": props, "schedulingPeriod": "1 day"}}})
        await nifi.post(f"/process-groups/{gid}/connections", {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {"source": {"id": gen["id"], "groupId": gid, "type": "PROCESSOR"},
                          "destination": {"id": destination_id, "groupId": gid, "type": CONNECTABLE[d_kind]},
                          "selectedRelationships": ["success"]}})
    else:
        if gen["component"]["state"] == "DISABLED":
            gen = (await nifi.request("PUT", f"/processors/{gen['id']}/run-status",
                                      json={"revision": _rev(gen), "state": "STOPPED"})).json()
        gen = (await nifi.request("PUT", f"/processors/{gen['id']}", json={
            "revision": _rev(gen), "component": {"id": gen["id"], "config": {"properties": props}}})).json()

    def sent_so_far(p: dict) -> int:
        return p["status"]["aggregateSnapshot"].get("flowFilesOut", 0)

    for _i in range(count):
        current = await nifi.get(f"/processors/{gen['id']}")
        before = sent_so_far(current)
        await nifi.request("PUT", f"/processors/{gen['id']}/run-status", json={"revision": _rev(current), "state": "RUN_ONCE"})

        # STOPPED alone is not enough: right after RUN_ONCE the processor can report STOPPED before its one
        # task has run, and a following injection would then overwrite the properties first.
        async def done() -> bool:
            p = await nifi.get(f"/processors/{gen['id']}")
            return (sent_so_far(p) > before and p["component"]["state"] == "STOPPED"
                    and p["status"]["aggregateSnapshot"].get("activeThreadCount", 0) == 0)
        await _wait(done)
    current = await nifi.get(f"/processors/{gen['id']}")
    errors = current["component"].get("validationErrors")
    if not errors:
        # Disabled, so starting the whole group later does not fire the injector once more.
        await nifi.request("PUT", f"/processors/{gen['id']}/run-status", json={"revision": _rev(current), "state": "DISABLED"})
    return {"injector_id": gen["id"], "destination_id": destination_id, "sent": count if not errors else 0,
            "validation_errors": errors,
            "next": "Run the destination (set_run_state RUNNING or RUN_ONCE), then list_queue on its outgoing connections and get_bulletins."}


async def _processor_idle(nifi: NiFiClient, pid: str) -> bool:
    p = await nifi.get(f"/processors/{pid}")
    return p["component"]["state"] != "RUNNING" and p["status"]["aggregateSnapshot"].get("activeThreadCount", 0) == 0


async def _service_state(nifi: NiFiClient, sid: str, state: str) -> bool:
    return (await nifi.get(f"/controller-services/{sid}"))["component"]["state"] == state


async def _prepare_group_delete(nifi: NiFiClient, group_id: str) -> None:
    await nifi.request("PUT", f"/flow/process-groups/{group_id}", json={"id": group_id, "state": "STOPPED"})

    async def idle() -> bool:
        s = (await nifi.get(f"/flow/process-groups/{group_id}/status", recursive="true"))["processGroupStatus"]["aggregateSnapshot"]
        return s.get("activeThreadCount", 0) == 0
    await _wait(idle)
    services = (await nifi.get(f"/flow/process-groups/{group_id}/controller-services",
                               includeAncestorGroups="false", includeDescendantGroups="true"))["controllerServices"]
    for s in services:
        if s["component"]["state"] != "DISABLED":
            await nifi.request("PUT", f"/controller-services/{s['id']}/run-status", json={"revision": _rev(s), "state": "DISABLED"})
    for s in services:
        await _wait(lambda sid=s["id"]: _service_state(nifi, sid, "DISABLED"))

    async def drop_all(gid: str) -> None:
        flow = (await nifi.get(f"/flow/process-groups/{gid}"))["processGroupFlow"]["flow"]
        for c in flow["connections"]:
            await _drop_queue(nifi, c["id"])
        for g in flow["processGroups"]:
            await drop_all(g["id"])
    await drop_all(group_id)


async def _delete_group(nifi: NiFiClient, group_id: str) -> None:
    await _prepare_group_delete(nifi, group_id)
    current = await nifi.get(f"/process-groups/{group_id}")
    await nifi.request("DELETE", f"/process-groups/{group_id}", params={"version": current["revision"]["version"], "clientId": CLIENT_ID})
