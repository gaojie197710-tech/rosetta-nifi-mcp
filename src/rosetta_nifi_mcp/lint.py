"""check_flow_design: find where a flow departs from the design guide, before it bites at run time.

Rules (see get_design_guide topic "gotchas" and "layout"):
- parameter_context_missing: a group whose processors reference #{...} has no parameter context
  (groups created through the API do not inherit their parent's).
- parameter_in_quoted_el: #{...} inside a quoted Expression Language literal is never substituted.
- invokehttp_retry_penalty: InvokeHTTP feeding a RetryFlowFile with InvokeHTTP's default 30 sec penalty.
- overlapping_connections: two or more connections between the same components with the same bends draw
  as one line.
- undocumented_auto_terminate: a failure-type relationship auto-terminated with no comment saying why.
"""

from __future__ import annotations

import re
from collections import defaultdict

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations

from .client import NiFiClient

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

QUOTED_PARAM = re.compile(r"""(['"])[^'"\n]*#\{[^}]+\}[^'"\n]*\1""")
FAILURE_RELATIONSHIPS = {"failure", "Failure", "Retry", "No Retry", "retries_exceeded", "timeout"}
DEFAULT_PENALTY = "30 sec"


def el_expressions(value: str) -> list[str]:
    """The text of each top-level ${...} expression in a property value (nested braces included)."""
    out, i = [], 0
    while (start := value.find("${", i)) != -1:
        depth, j = 0, start + 1
        while j < len(value):
            if value[j] == "{":
                depth += 1
            elif value[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        out.append(value[start:j + 1])
        i = j + 1
    return out


def quoted_parameters(value: str) -> list[str]:
    return [m.group(0) for expr in el_expressions(value) for m in QUOTED_PARAM.finditer(expr)]


def _endpoint(end: dict, group_id: str) -> str:
    # A port inside a child group is drawn as that child group on this canvas.
    return end["id"] if end["groupId"] == group_id else end["groupId"]


def overlapping(connections: list[dict], group_id: str) -> list[list[dict]]:
    """Sets of connections drawn on top of each other: same two endpoints (either direction), same bends."""
    by_route: dict[tuple, list[dict]] = defaultdict(list)
    for c in connections:
        comp = c["component"]
        s, d = _endpoint(comp["source"], group_id), _endpoint(comp["destination"], group_id)
        if s == d:
            continue  # self-loops are drawn with their own automatic bends
        bends = tuple((round(b["x"]), round(b["y"])) for b in comp.get("bends") or [])
        by_route[(frozenset((s, d)), bends)].append(c)
    return [cs for cs in by_route.values() if len(cs) > 1]


async def lint_group(nifi: NiFiClient, group_id: str = "root", recursive: bool = True,
                     path: str | None = None, parent_context: str | None = None) -> list[dict]:
    pgf = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]
    flow = pgf["flow"]
    path = path if path is not None else (pgf["breadcrumb"]["breadcrumb"]["name"] if group_id != "root" else "")
    context = (pgf.get("parameterContext") or {}).get("component", {}).get("name") or (pgf.get("parameterContext") or {}).get("id")
    warnings: list[dict] = []

    def warn(rule: str, component: str, message: str, fix: str) -> None:
        warnings.append({"rule": rule, "group": path or "/", "group_id": group_id, "component": component,
                         "message": message, "fix": fix})

    procs = {p["id"]: p for p in flow["processors"]}
    uses_params = [p["component"]["name"] for p in flow["processors"]
                   if any("#{" in (v or "") for v in p["component"]["config"]["properties"].values())]
    if uses_params and not context:
        inherited = f" (its parent uses {parent_context!r})" if parent_context else ""
        warn("parameter_context_missing", ", ".join(uses_params[:5]),
             f"{len(uses_params)} processor(s) reference parameters but the group has no parameter context{inherited}",
             "Bind the context to this group: groups created through the API do not inherit it.")

    for p in flow["processors"]:
        comp = p["component"]
        cfg = comp["config"]
        for prop, value in cfg["properties"].items():
            for hit in quoted_parameters(value or ""):
                warn("parameter_in_quoted_el", comp["name"],
                     f"property {prop!r}: {hit} is a quoted literal, so the parameter is never substituted",
                     "Copy the parameter into an attribute (UpdateAttribute value cfg.x = #{X}) and use ${cfg.x} unquoted.")
        auto = [r for r in cfg.get("autoTerminatedRelationships") or [] if r in FAILURE_RELATIONSHIPS]
        if auto and not (cfg.get("comments") or "").strip():
            warn("undocumented_auto_terminate", comp["name"],
                 f"auto-terminates {auto} with no comment saying why",
                 "Route the relationship to failure handling, or say in the processor's comments why dropping it is safe.")

    for c in flow["connections"]:
        comp = c["component"]
        src, dst = procs.get(comp["source"]["id"]), procs.get(comp["destination"]["id"])
        if not src or not dst:
            continue
        if (src["component"]["type"].endswith(".InvokeHTTP") and dst["component"]["type"].endswith(".RetryFlowFile")
                and {"Retry", "Failure"} & set(comp.get("selectedRelationships") or [])
                and src["component"]["config"].get("penaltyDuration") == DEFAULT_PENALTY):
            warn("invokehttp_retry_penalty", src["component"]["name"],
                 f"retries via {dst['component']['name']} wait InvokeHTTP's default {DEFAULT_PENALTY} penalty each time",
                 "Set the InvokeHTTP penalty duration explicitly (e.g. 5 sec).")

    for cs in overlapping(flow["connections"], group_id):
        names = [f"{c['component']['source']['name']} -> {c['component']['destination']['name']}"
                 + (f" [{', '.join(c['component'].get('selectedRelationships') or [])}]" if c["component"].get("selectedRelationships") else "")
                 for c in cs]
        warn("overlapping_connections", "; ".join(names[:4]),
             f"{len(cs)} connections between the same components share the same route and draw as one line",
             "Give each its own bend point (build_flow stack/grid layouts do), or layout_process_group.")

    if recursive:
        for g in flow["processGroups"]:
            child_path = f"{path}/{g['component']['name']}"
            warnings += await lint_group(nifi, g["id"], True, child_path, context or parent_context)
    return warnings


def summarize(warnings: list[dict], limit: int) -> dict:
    by_rule: dict[str, int] = defaultdict(int)
    for w in warnings:
        by_rule[w["rule"]] += 1
    return {"clean": not warnings, "total": len(warnings), "by_rule": dict(by_rule), "warnings": warnings[:limit],
            **({"truncated": len(warnings) - limit} if len(warnings) > limit else {})}


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def check_flow_design(ctx: Context, group_id: str = "root", recursive: bool = True, limit: int = 50) -> dict:
        """Check a process group (and its children) against the design guide: groups missing their parameter
        context, #{param} inside quoted Expression Language (never substituted), InvokeHTTP retries waiting the
        default 30 sec penalty, overlapping connections (NiFi draws them as one line) and failure relationships
        auto-terminated without a comment. Each warning names the group, component, problem and fix."""
        nifi: NiFiClient = ctx.request_context.lifespan_context
        return summarize(await lint_group(nifi, group_id, recursive), limit)


async def design_section(nifi: NiFiClient, limit: int) -> tuple[dict, list[str]]:
    """health_check's design section and its issue line."""
    s = summarize(await lint_group(nifi, "root"), limit)
    issues = [f"{s['total']} flow design warning(s): "
              + ", ".join(f"{n} {r}" for r, n in sorted(s["by_rule"].items()))] if s["total"] else []
    return s, issues
