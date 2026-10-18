"""Parameter contexts: list, create, update (async update request) and assign to process groups.
Sensitive parameter values are write-only: they are never returned."""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from .build import CLIENT_ID, WRITE, _nifi
from .client import NiFiClient
from .registry import _run_update_request

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=False)


class Parameter(BaseModel):
    name: str
    value: str | None = Field(None, description="null removes the parameter on update")
    sensitive: bool = False
    description: str | None = None


def _param_entities(params: list[Parameter], remove_nulls: bool) -> list[dict]:
    out = []
    for p in params:
        if p.value is None and remove_nulls:
            out.append({"parameter": {"name": p.name}})
            continue
        body: dict[str, Any] = {"name": p.name, "value": p.value, "sensitive": p.sensitive}
        if p.description is not None:
            body["description"] = p.description
        out.append({"parameter": body})
    return out


def context_summary(c: dict) -> dict:
    comp = c["component"]
    params = []
    for pe in comp.get("parameters", []):
        p = pe["parameter"]
        item = {"name": p["name"], "sensitive": p.get("sensitive", False), "description": p.get("description")}
        if not p.get("sensitive"):
            item["value"] = p.get("value")
        params.append(item)
    return {"id": c["id"], "name": comp["name"], "description": comp.get("description"),
            "parameters": sorted(params, key=lambda p: p["name"]),
            "bound_process_groups": [g["id"] for g in comp.get("boundProcessGroups", [])]}


async def create_context(nifi: NiFiClient, name: str, parameters: list[Parameter], description: str | None = None) -> dict:
    component: dict[str, Any] = {"name": name, "parameters": _param_entities(parameters, remove_nulls=False)}
    if description:
        component["description"] = description
    return await nifi.post("/parameter-contexts", {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": component})


async def assign_context(nifi: NiFiClient, group_id: str, context_id: str | None) -> dict:
    pg = await nifi.get(f"/process-groups/{group_id}")
    return (await nifi.request("PUT", f"/process-groups/{group_id}", json={
        "revision": {"clientId": CLIENT_ID, "version": pg["revision"]["version"]},
        "component": {"id": group_id, "parameterContext": {"id": context_id}}})).json()


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def list_parameter_contexts(ctx: Context) -> dict:
        """Parameter contexts with their parameters and bound process groups. Sensitive values are never shown."""
        cs = (await _nifi(ctx).get("/flow/parameter-contexts"))["parameterContexts"]
        return {"parameter_contexts": [context_summary(c) for c in cs if c.get("component")]}

    @mcp.tool(annotations=WRITE)
    async def create_parameter_context(ctx: Context, name: str, parameters: list[Parameter],
                                       description: str | None = None) -> dict:
        """Create a parameter context. Processors reference parameters as #{name}; a sensitive parameter
        can only be used in sensitive properties. Assign it to a group with assign_parameter_context."""
        return context_summary(await create_context(_nifi(ctx), name, parameters, description))

    @mcp.tool(annotations=WRITE)
    async def update_parameter_context(ctx: Context, context_id: str, parameters: list[Parameter]) -> dict:
        """Add, change or remove (value null) parameters. Parameters not listed are left unchanged.
        NiFi stops and restarts the processors that reference changed parameters while it applies this."""
        nifi = _nifi(ctx)
        current = await nifi.get(f"/parameter-contexts/{context_id}")
        existing = {pe["parameter"]["name"] for pe in current["component"].get("parameters", [])}
        missing = [p.name for p in parameters if p.value is None and p.name not in existing]
        if missing:
            raise ToolError(f"Cannot remove parameters that do not exist: {missing}")
        await _run_update_request(nifi, f"/parameter-contexts/{context_id}/update-requests", {
            "id": context_id,
            "revision": {"clientId": CLIENT_ID, "version": current["revision"]["version"]},
            "component": {"id": context_id, "parameters": _param_entities(parameters, remove_nulls=True)}})
        return context_summary(await nifi.get(f"/parameter-contexts/{context_id}"))

    @mcp.tool(annotations=WRITE)
    async def assign_parameter_context(ctx: Context, group_id: str, context_id: str | None) -> dict:
        """Bind a parameter context to a process group (context_id null unbinds it)."""
        g = await assign_context(_nifi(ctx), group_id, context_id)
        pc = g["component"].get("parameterContext")
        return {"group_id": group_id, "parameter_context": {"id": pc["id"], "name": pc["component"]["name"]} if pc else None}

    @mcp.tool(annotations=DESTRUCTIVE)
    async def delete_parameter_context(ctx: Context, context_id: str) -> dict:
        """Delete a parameter context that no process group is bound to."""
        nifi = _nifi(ctx)
        current = await nifi.get(f"/parameter-contexts/{context_id}")
        if current["component"].get("boundProcessGroups"):
            raise ToolError("Context is still bound to process groups; unbind them first")
        await nifi.request("DELETE", f"/parameter-contexts/{context_id}",
                           params={"version": current["revision"]["version"], "clientId": CLIENT_ID})
        return {"deleted": context_id}
