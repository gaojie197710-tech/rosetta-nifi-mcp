"""Filters, paging and counts shared by the tools that list components, so very large groups stay compact."""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal

Status = Literal["invalid", "stopped", "running", "disabled", "erroring", "back_pressured"]
DEFAULT_LIMIT = 25


def is_back_pressured(conn: dict, near: int = 80) -> bool:
    s = conn.get("status", {}).get("aggregateSnapshot", {})
    return max(s.get("percentUseCount") or 0, s.get("percentUseBytes") or 0) >= near


def processor_flags(procs: list[dict], conns: list[dict], erroring_ids: set[str]) -> dict[str, set[str]]:
    """Processor id -> the status flags it matches."""
    pressured = set()
    for c in conns:
        if is_back_pressured(c):
            pressured |= {c["component"]["source"]["id"], c["component"]["destination"]["id"]}
    flags: dict[str, set[str]] = {}
    for p in procs:
        comp = p["component"]
        f = set()
        if comp["state"] == "INVALID" or comp.get("validationErrors"):
            f.add("invalid")
        if comp["state"] in ("STOPPED", "RUNNING", "DISABLED"):
            f.add(comp["state"].lower())
        if p["id"] in erroring_ids:
            f.add("erroring")
        if p["id"] in pressured:
            f.add("back_pressured")
        flags[p["id"]] = f
    return flags


def select(procs: list[dict], flags: dict[str, set[str]], status: str | None = None,
           name: str | None = None, type_: str | None = None) -> list[dict]:
    out = procs
    if status:
        out = [p for p in out if status in flags.get(p["id"], set())]
    if name:
        out = [p for p in out if name.lower() in p["component"]["name"].lower()]
    if type_:
        out = [p for p in out if type_.lower() in p["component"]["type"].lower()]
    return out


def counts(procs: list[dict], flags: dict[str, set[str]]) -> dict[str, Any]:
    by_flag = Counter(f for p in procs for f in flags.get(p["id"], set()))
    by_type = Counter(p["component"]["type"].rsplit(".", 1)[-1] for p in procs)
    return {"processors": len(procs), **{k: by_flag[k] for k in ("running", "stopped", "invalid", "disabled", "erroring", "back_pressured") if by_flag[k]},
            "top_types": dict(by_type.most_common(8))}


def page(items: list, limit: int, offset: int, what: str, hint: str) -> tuple[list, dict | None]:
    """A slice of items plus a paging note when anything was left out."""
    sliced = items[offset:offset + limit]
    if offset == 0 and len(items) <= limit:
        return sliced, None
    note = {"what": what, "total": len(items), "offset": offset, "returned": len(sliced)}
    if offset + limit < len(items):
        note["next_offset"] = offset + limit
    note["hint"] = hint
    return sliced, note
