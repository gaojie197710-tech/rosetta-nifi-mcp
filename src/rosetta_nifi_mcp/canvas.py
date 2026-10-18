"""Canvas geometry as NiFi 2.x draws it, for checking a layout without a browser: component boxes, connection
lines and connection label boxes, and where they collide.

Measured on the NiFi 2.x canvas: a line runs from box edge to box edge along the line between the box centres
(or the next bend); its label is 240 wide and 19 per row + 3 tall, centred on the bend that carries it
(labelIndex) or, without bends, on the middle of the line. Rows: From (source is a port of a child group),
To (destination is a port of a child group), Name (relationships or a connection name), Queued (always).
"""

from __future__ import annotations

from .layout import FLOW_KEYS, SIZE

LABEL_W = 240
LABEL_ROW = 19

Box = tuple[float, float, float, float]  # x0, y0, x1, y1
Point = tuple[float, float]


def _clip(box: Box, toward: Point) -> Point:
    """Where the line from the box centre toward a point leaves the box."""
    x0, y0, x1, y1 = box
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    dx, dy = toward[0] - cx, toward[1] - cy
    if dx == 0 and dy == 0:
        return cx, cy
    t = min((x1 - cx) / abs(dx) if dx else float("inf"), (y1 - cy) / abs(dy) if dy else float("inf"), 1.0)
    return cx + dx * t, cy + dy * t


def _segment_hits(a: Point, b: Point, box: Box) -> bool:
    """Does segment a-b pass through the box (Liang-Barsky)?"""
    x0, y0, x1, y1 = box
    t0, t1 = 0.0, 1.0
    dx, dy = b[0] - a[0], b[1] - a[1]
    for p, q in ((-dx, a[0] - x0), (dx, x1 - a[0]), (-dy, a[1] - y0), (dy, y1 - a[1])):
        if p == 0:
            if q < 0:
                return False
            continue
        r = q / p
        if p < 0:
            t0 = max(t0, r)
        else:
            t1 = min(t1, r)
        if t0 > t1:
            return False
    return True


def _overlap(a: Box, b: Box, margin: float = 0) -> bool:
    return a[0] < b[2] + margin and b[0] < a[2] + margin and a[1] < b[3] + margin and b[1] < a[3] + margin


def geometry(flow: dict) -> tuple[dict[str, tuple[str, Box]], list[dict]]:
    """flow: a processGroupFlow's "flow". Returns {id: (name, box)} and one dict per connection with its
    endpoints, polyline points and label box."""
    gid_of = {}
    boxes: dict[str, tuple[str, Box]] = {}
    for kind, key in FLOW_KEYS.items():
        for e in flow.get(key, []):
            w, h = SIZE[kind]
            x, y = e["position"]["x"], e["position"]["y"]
            boxes[e["id"]] = ((e.get("component") or {}).get("name", kind), (x, y, x + w, y + h))
            gid_of[e["id"]] = (e.get("component") or {}).get("parentGroupId")
    lines = []
    for c in flow.get("connections", []):
        comp = c["component"]
        here = comp["parentGroupId"]

        def owner(end: dict) -> str:
            return end["id"] if end["groupId"] == here else end["groupId"]
        s, d = owner(comp["source"]), owner(comp["destination"])
        if s not in boxes or d not in boxes:
            continue
        bends = [(b["x"], b["y"]) for b in comp.get("bends") or []]
        sb, db = boxes[s][1], boxes[d][1]
        center = lambda b: ((b[0] + b[2]) / 2, (b[1] + b[3]) / 2)  # noqa: E731
        start = _clip(sb, bends[0] if bends else center(db))
        end = _clip(db, bends[-1] if bends else center(sb))
        points = [start, *bends, end]
        rows = 1 + (comp["source"]["groupId"] != here) + (comp["destination"]["groupId"] != here)
        rows += bool(comp.get("selectedRelationships") or comp.get("name"))
        h = LABEL_ROW * rows + 3
        if bends:
            lx, ly = bends[min(comp.get("labelIndex") or 0, len(bends) - 1)]
        else:
            lx, ly = (start[0] + end[0]) / 2, (start[1] + end[1]) / 2
        name = f"{boxes[s][0]} -> {boxes[d][0]}"
        if comp.get("selectedRelationships"):
            name += f" [{', '.join(comp['selectedRelationships'])}]"
        lines.append({"id": c["id"], "name": name, "src": s, "dst": d, "points": points,
                      "label": (lx - LABEL_W / 2, ly - h / 2, lx + LABEL_W / 2, ly + h / 2)})
    return boxes, lines


def clutter(flow: dict) -> list[str]:
    """Everything on one canvas that collides: boxes overlapping boxes, labels overlapping boxes or other
    labels, and lines running through a component they do not connect."""
    boxes, lines = geometry(flow)
    found = []
    ids = list(boxes)
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            if _overlap(boxes[a][1], boxes[b][1]):
                found.append(f"box {boxes[a][0]!r} overlaps box {boxes[b][0]!r}")
    for i, ln in enumerate(lines):
        for n, (name, box) in boxes.items():
            if _overlap(ln["label"], box):
                found.append(f"label of {ln['name']} overlaps {name!r}")
            if n not in (ln["src"], ln["dst"]) and any(
                    _segment_hits(p, q, box) for p, q in zip(ln["points"], ln["points"][1:])):
                found.append(f"line {ln['name']} runs through {name!r}")
        for other in lines[i + 1:]:
            if _overlap(ln["label"], other["label"]):
                found.append(f"label of {ln['name']} overlaps label of {other['name']}")
    return found
