"""Canvas layout: tidy a process group into non-overlapping top-to-bottom layers, and find free space
for new components so they are not all stacked at (0, 0)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from mcp.server.mcpserver import Context, MCPServer

from .build import CLIENT_ID, WRITE, _nifi
from .client import NiFiClient

# Rendered box sizes on the NiFi 2.x canvas.
SIZE = {
    "processor": (352, 128), "process_group": (384, 176), "remote_process_group": (384, 160),
    "input_port": (240, 48), "output_port": (240, 48), "funnel": (48, 48),
}
COL_GAP = 120   # horizontal space between boxes in a layer
ROW_GAP = 130   # vertical space between layers: room for the connection label box
COMPONENT_GAP = 200
PATHS = {"processor": "processors", "process_group": "process-groups", "remote_process_group": "remote-process-groups",
         "input_port": "input-ports", "output_port": "output-ports", "funnel": "funnels"}
FLOW_KEYS = {"processor": "processors", "process_group": "processGroups", "remote_process_group": "remoteProcessGroups",
             "input_port": "inputPorts", "output_port": "outputPorts", "funnel": "funnels"}


async def _nodes_and_edges(nifi: NiFiClient, group_id: str) -> tuple[dict[str, dict], list[tuple[str, str, dict]], dict]:
    flow = (await nifi.get(f"/flow/process-groups/{group_id}"))["processGroupFlow"]
    gid, f = flow["id"], flow["flow"]
    nodes: dict[str, dict] = {}
    for kind, key in FLOW_KEYS.items():
        for e in f.get(key, []):
            w, h = SIZE[kind]
            nodes[e["id"]] = {"id": e["id"], "kind": kind, "w": w, "h": h, "pos": e["position"], "revision": e["revision"],
                              "name": (e.get("component") or {}).get("name", kind)}

    def owner(end: dict) -> str | None:
        # Ports inside a child group (or a remote group) are drawn as that group on this canvas.
        return end["id"] if end["groupId"] == gid else (end["groupId"] if end["groupId"] in nodes else None)

    edges = []
    for c in f.get("connections", []):
        s, d = owner(c["component"]["source"]), owner(c["component"]["destination"])
        if s in nodes and d in nodes:
            edges.append((s, d, c))
    return nodes, edges, flow


def _layers(nodes: dict[str, dict], edges: list[tuple[str, str, dict]]) -> tuple[list[list[list[str]]], set[str]]:
    """Weakly connected components, each split into layers (longest path from its sources).
    Returns the components and the ids of connections that were treated as back edges (loops)."""
    out, undirected = defaultdict(list), defaultdict(set)
    for s, d, c in edges:
        undirected[s].add(d); undirected[d].add(s)
        if s != d:
            out[s].append((d, c["id"]))

    # Break cycles with a DFS from the nodes that have no incoming edges (then any leftovers).
    indeg = defaultdict(int)
    for s, d, _ in edges:
        if s != d:
            indeg[d] += 1
    order = sorted(nodes, key=lambda n: (indeg[n] > 0, nodes[n]["pos"]["y"], nodes[n]["pos"]["x"]))
    back: set[str] = set()
    state: dict[str, int] = {}
    for root in order:
        if root in state:
            continue
        stack = [(root, iter(out[root]))]
        state[root] = 1
        while stack:
            node, it = stack[-1]
            nxt = next(it, None)
            if nxt is None:
                state[node] = 2; stack.pop(); continue
            child, cid = nxt
            if state.get(child) == 1:
                back.add(cid)
            elif child not in state:
                state[child] = 1; stack.append((child, iter(out[child])))
    dag = {n: [d for d, cid in out[n] if cid not in back] for n in nodes}

    # Longest-path layering over the DAG.
    layer: dict[str, int] = {}
    def depth(n: str, seen: frozenset = frozenset()) -> int:
        if n in layer:
            return layer[n]
        preds = [s for s in nodes if n in dag[s]]
        layer[n] = 0 if not preds else 1 + max(depth(p) for p in preds)
        return layer[n]
    for n in nodes:
        depth(n)

    # Components, ordered left to right by their current position.
    seen: set[str] = set()
    comps = []
    for n in sorted(nodes, key=lambda n: (nodes[n]["pos"]["x"], nodes[n]["pos"]["y"])):
        if n in seen:
            continue
        comp, todo = [], [n]
        while todo:
            x = todo.pop()
            if x in seen:
                continue
            seen.add(x); comp.append(x); todo.extend(undirected[x] - seen)
        base = min(layer[x] for x in comp)
        rows: dict[int, list[str]] = defaultdict(list)
        for x in comp:
            rows[layer[x] - base].append(x)
        ordered = [sorted(rows[i], key=lambda x: nodes[x]["pos"]["x"]) for i in range(max(rows) + 1)]
        # Two barycenter sweeps to reduce crossings.
        preds = {x: [s for s in comp if x in dag[s]] for x in comp}
        for _ in range(2):
            for i in range(1, len(ordered)):
                idx = {x: k for k, x in enumerate(ordered[i - 1])}
                ordered[i].sort(key=lambda x: sum(idx[p] for p in preds[x] if p in idx) / max(1, sum(p in idx for p in preds[x])) if preds[x] else 1e9)
        comps.append(ordered)
    return comps, back


SIDE_SINKS = ("funnel", "output_port")
SIDE_COLUMN = 300   # feeder's right edge to its side sinks: a connection label (240) fits in between
SIDE_STEP = 70      # vertical distance between side sinks of one feeder (a label is 41 tall)
LANE_STEP = 280     # distance between shared side lanes
LANE_ROW = 50       # vertical distance between one feeder's lines into different lanes


def _is_side(c: dict) -> bool:
    """A connection that carries something other than the main result: it names relationships or comes out of
    a child group's port, and none of them is success."""
    comp = c.get("component") or {}
    names = list(comp.get("selectedRelationships") or [])
    src = comp.get("source") or {}
    if src.get("type") == "OUTPUT_PORT" and src.get("groupId") != comp.get("parentGroupId"):
        names.append(src.get("name", ""))
    return bool(names) and "success" not in names


def _side_sinks(nodes: dict[str, dict], edges: list[tuple[str, str, dict]]) -> dict[str, list[str]]:
    """Funnels and output ports that only take side connections and feed nothing: sink -> its feeders."""
    feeders: dict[str, list[str]] = defaultdict(list)
    side_only: dict[str, bool] = defaultdict(lambda: True)
    has_out = {s for s, d, _ in edges if s != d}
    for s, d, c in edges:
        if s != d and s not in feeders[d]:
            feeders[d].append(s)
        side_only[d] &= _is_side(c)
    return {n: feeders[n] for n in nodes if nodes[n]["kind"] in SIDE_SINKS and n not in has_out
            and feeders[n] and side_only[n] and n not in feeders[n]}


def plan_layout(nodes: dict[str, dict], edges: list[tuple[str, str, dict]]) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    """Layers top to bottom. Side sinks (a failure or ledger funnel, say) stay out of the layers so the main
    path reads straight down: a sink with one feeder sits in a column right of it, a sink shared by several
    feeders at the foot of a vertical lane right of everything. A feeder must be last in its row, so its
    lines run right into free space; otherwise its sinks stay in the layers."""
    if not nodes:
        return {}, {}
    side = _side_sinks(nodes, edges)
    while True:
        core = [e for e in edges if e[0] not in side and e[1] not in side]
        comps, back = _layers({n: v for n, v in nodes.items() if n not in side}, core)
        comp_of = {n: i for i, comp in enumerate(comps) for row in comp for n in row}
        last_in_row = {row[-1] for comp in comps for row in comp}
        bad = {n for n, fs in side.items()
               if any(f in side or f not in last_in_row for f in fs) or len({comp_of[f] for f in fs}) > 1}
        if not bad:
            break
        side = {n: fs for n, fs in side.items() if n not in bad}

    x0 = min(n["pos"]["x"] for n in nodes.values())
    y0 = min(n["pos"]["y"] for n in nodes.values())
    positions: dict[str, dict] = {}
    comp_right: dict[str, float] = {}
    side_bends: dict[str, list[dict]] = {}
    x_cursor = x0
    for ci, comp in enumerate(comps):
        width = max(sum(nodes[n]["w"] for n in row) + COL_GAP * (len(row) - 1) for row in comp)
        y = y0
        for row in comp:
            row_w = sum(nodes[n]["w"] for n in row) + COL_GAP * (len(row) - 1)
            x = x_cursor + (width - row_w) / 2
            row_h = max(nodes[n]["h"] for n in row)
            for n in row:
                positions[n] = {"x": round(x), "y": round(y + (row_h - nodes[n]["h"]) / 2)}
                x += nodes[n]["w"] + COL_GAP
            y += row_h + ROW_GAP
        for row in comp:
            for n in row:
                comp_right[n] = x_cursor + width
        right = x_cursor + width
        layer_of = {n: i for i, row in enumerate(comp) for n in row}
        if any(c["id"] in back or abs(layer_of[d] - layer_of[s]) > 1
               for s, d, c in core if s in layer_of and d in layer_of and s != d):
            right += 120   # room for the lane that loops and long edges take (comp_right + 60)
        mine = sorted((n for n, fs in side.items() if comp_of[fs[0]] == ci), key=lambda n: nodes[n]["name"])
        # One feeder: a column beside it, one sink per row, each line bending where its label sits.
        column_x = right + SIDE_COLUMN
        col_right = right
        for f in [n for row in comp for n in row]:
            own = [n for n in mine if side[n] == [f]]
            fp = positions[f]
            for k, n in enumerate(own):
                cy = fp["y"] + 24 + SIDE_STEP * k
                positions[n] = {"x": round(column_x), "y": round(cy - nodes[n]["h"] / 2)}
                col_right = max(col_right, column_x + nodes[n]["w"])
                for s, d, c in edges:
                    if s == f and d == n:
                        side_bends[c["id"]] = [{"x": round(column_x - SIDE_COLUMN / 2), "y": round(cy)}]
        # Shared: one vertical lane per sink; each feeder's line bends into it at its own height (the label
        # sits on that bend), and the sink waits at the lane's foot below everything.
        shared = [n for n in mine if len(side[n]) > 1]
        lane_x = (col_right if col_right > right else right) + 150
        # At the last row's height when no feeder sits in it, else below it.
        last_row = comp[-1]
        bottom = y if any(f in last_row for n in shared for f in side[n]) else positions[last_row[0]]["y"]
        for k, n in enumerate(shared):
            lx = lane_x + LANE_STEP * k
            positions[n] = {"x": round(lx - nodes[n]["w"] / 2), "y": round(bottom)}
            for s, d, c in edges:
                if d == n:
                    j = sorted((m for m in shared if s in side[m]), key=shared.index).index(n)
                    side_bends[c["id"]] = [{"x": round(lx), "y": round(positions[s]["y"] + 16 + LANE_ROW * j)}]
        if shared:
            right = lane_x + LANE_STEP * (len(shared) - 1) + 140
        right = max(right, col_right)
        for n in mine:
            comp_right[n] = right
        x_cursor = right + COMPONENT_GAP

    # Straight lines where possible; loops and edges that skip layers go around the right side.
    layer_of = {n: i for comp in comps for i, row in enumerate(comp) for n in row}
    bends: dict[str, list[dict]] = {}
    for s, d, c in edges:
        if c["id"] in side_bends:
            bends[c["id"]] = side_bends[c["id"]]
            continue
        ps, pd = positions[s], positions[d]
        sx, sy = ps["x"] + nodes[s]["w"] / 2, ps["y"] + nodes[s]["h"] / 2
        dx, dy = pd["x"] + nodes[d]["w"] / 2, pd["y"] + nodes[d]["h"] / 2
        lane = comp_right[s] + 60
        if s == d:
            bends[c["id"]] = [{"x": ps["x"] + nodes[s]["w"] + 60, "y": sy - 40}, {"x": ps["x"] + nodes[s]["w"] + 60, "y": sy + 40}]
        elif c["id"] in back or abs(layer_of[d] - layer_of[s]) > 1:
            bends[c["id"]] = [{"x": lane, "y": sy}, {"x": lane, "y": dy}]
        else:
            bends[c["id"]] = []
    # Several straight connections between the same two components would draw as one line: spread them.
    from .flowplan import stagger
    same: dict[frozenset, list] = defaultdict(list)
    for s, d, c in edges:
        if s != d and not bends[c["id"]]:
            same[frozenset((s, d))].append((s, d, c))
    for group in same.values():
        mid = {n: (positions[n]["x"] + nodes[n]["w"] / 2, positions[n]["y"] + nodes[n]["h"] / 2) for n in (group[0][0], group[0][1])}
        spread = stagger([(c["id"], mid[s], mid[d]) for s, d, c in group])
        for cid, pts in spread.items():
            bends[cid] = [{"x": x, "y": y} for x, y in pts]
    return positions, bends


HEADER_MAX_HEIGHT = 60   # labels this thin are section headers; taller ones are notes and stay put
HEADER_GAP = 80          # header bar sits this far above its section's first row
SECTION_GAP = 160


def _assign_sections(nodes: dict[str, dict], edges: list[tuple[str, str, dict]], headers: list[dict]) -> dict[str, str | None]:
    """Each component belongs to the nearest header above it whose column covers it. A header's column runs
    from its x to the next header on the same row (or a little past its own width for the last one).
    Components left unassigned, or with no neighbour in their own section, follow their neighbours."""
    rows: dict[float, list[dict]] = defaultdict(list)
    for h in headers:
        rows[round(h["position"]["y"])].append(h)
    extent = {}
    for row in rows.values():
        row.sort(key=lambda h: h["position"]["x"])
        for i, h in enumerate(row):
            x0 = h["position"]["x"]
            x1 = row[i + 1]["position"]["x"] if i + 1 < len(row) else x0 + h["component"]["width"] + 300
            extent[h["id"]] = (x0, x1)
    section: dict[str, str | None] = {}
    for n, v in nodes.items():
        cx, top = v["pos"]["x"] + v["w"] / 2, v["pos"]["y"]
        above = [h for h in headers if h["position"]["y"] <= top and extent[h["id"]][0] <= cx < extent[h["id"]][1]]
        section[n] = max(above, key=lambda h: h["position"]["y"])["id"] if above else None
    neighbours = defaultdict(list)
    for s, d, _ in edges:
        if s != d:
            neighbours[s].append(d); neighbours[d].append(s)
    for _ in range(3):
        for n in nodes:
            near = [section[m] for m in neighbours[n] if section[m] is not None]
            if near and (section[n] is None or section[n] not in near):
                section[n] = max(set(near), key=near.count)
    return section


def plan_sectioned_layout(nodes: dict[str, dict], edges: list[tuple[str, str, dict]], labels: list[dict]) -> tuple[dict, dict, dict]:
    """Lay out each header-labelled section on its own, pack sections without overlap in their original
    arrangement, and move each header to sit on top of its section. Returns node positions, connection
    bends and label updates {label_id: {x, y, width}}."""
    headers = [l for l in labels if l["component"]["height"] <= HEADER_MAX_HEIGHT]
    if not headers:
        positions, bends = plan_layout(nodes, edges)
        return positions, bends, {}
    section = _assign_sections(nodes, edges, headers)
    by_id = {h["id"]: h for h in headers}
    members: dict[str | None, list[str]] = defaultdict(list)
    for n, sec in section.items():
        members[sec].append(n)

    order = sorted((h for h in headers if members.get(h["id"])), key=lambda h: (h["position"]["y"], h["position"]["x"]))
    placed: list[tuple[float, float, float, float]] = []   # x0, y0, x1, y1 including the header bar
    positions: dict[str, dict] = {}
    bends: dict[str, list[dict]] = {}
    label_updates: dict[str, dict] = {}
    row_right: dict[float, float] = {}

    def place(sec_nodes: list[str], want_x: float, want_y: float, min_width: float, row_key) -> tuple[float, float, float]:
        sub = {n: nodes[n] for n in sec_nodes}
        sub_edges = [e for e in edges if e[0] in sub and e[1] in sub]
        local, local_bends = plan_layout(sub, sub_edges)
        lx = min(p["x"] for p in local.values()); ly = min(p["y"] for p in local.values())
        w = max(max(local[n]["x"] + sub[n]["w"] for n in sub) - lx, min_width)
        h = max(local[n]["y"] + sub[n]["h"] for n in sub) - ly
        x = max(want_x, row_right.get(row_key, -1e9))
        y = want_y
        moved = True
        while moved:
            moved = False
            for (a0, b0, a1, b1) in placed:
                if x < a1 + SECTION_GAP and a0 < x + w + SECTION_GAP and y - HEADER_GAP < b1 + SECTION_GAP and b0 < y + h:
                    y = b1 + SECTION_GAP + HEADER_GAP; moved = True
        dx, dy = x - lx, y - ly
        for n, p in local.items():
            positions[n] = {"x": round(p["x"] + dx), "y": round(p["y"] + dy)}
        for cid, pts in local_bends.items():
            bends[cid] = [{"x": round(b["x"] + dx), "y": round(b["y"] + dy)} for b in pts]
        placed.append((x, y - HEADER_GAP, x + w, y + h))
        row_right[row_key] = x + w + SECTION_GAP
        return x, y, w

    for h in order:
        want_x = h["position"]["x"] + 20
        want_y = h["position"]["y"] + HEADER_GAP
        x, y, w = place(members[h["id"]], want_x, want_y, h["component"]["width"] - 40, round(h["position"]["y"]))
        label_updates[h["id"]] = {"x": round(x - 20), "y": round(y - HEADER_GAP), "width": round(w + 40)}
    if members.get(None):
        bottom = max((p[3] for p in placed), default=0)
        left = min((p[0] for p in placed), default=0)
        place(members[None], left, bottom + SECTION_GAP + HEADER_GAP, 0, "unlabelled")
    # Connections into a lower section turn at right angles just above that section's header, so they
    # do not cut diagonally across other boxes; other cross-section connections run straight.
    header_y = {n: label_updates[section[n]]["y"] for n in nodes if section[n] in label_updates}
    for s, d, c in edges:
        if c["id"] in bends:
            continue
        bends[c["id"]] = []
        if s in positions and d in positions and d in header_y and section[s] != section[d]:
            src_bottom = positions[s]["y"] + nodes[s]["h"]
            turn_y = header_y[d] - 40
            if src_bottom < turn_y:
                sx = positions[s]["x"] + nodes[s]["w"] / 2
                dx = positions[d]["x"] + nodes[d]["w"] / 2
                bends[c["id"]] = [{"x": round(sx), "y": turn_y}, {"x": round(dx), "y": turn_y}]
    return positions, bends, label_updates


async def free_position(nifi: NiFiClient, group_id: str, kind: str = "processor", near_id: str | None = None) -> tuple[float, float]:
    """A spot that overlaps nothing: below near_id if given, else below everything in the group."""
    nodes, _, _ = await _nodes_and_edges(nifi, group_id)
    if not nodes:
        return 0.0, 0.0
    w, _h = SIZE.get(kind, SIZE["processor"])
    if near_id and near_id in nodes:
        anchor = nodes[near_id]
        x = anchor["pos"]["x"] + (anchor["w"] - w) / 2
        y = anchor["pos"]["y"] + anchor["h"] + ROW_GAP
        # Slide right until clear of every existing box.
        def clashes(px: float, py: float) -> bool:
            return any(px < n["pos"]["x"] + n["w"] + 20 and n["pos"]["x"] < px + w + 20 and
                       py < n["pos"]["y"] + n["h"] + 20 and n["pos"]["y"] < py + _h + 20 for n in nodes.values())
        while clashes(x, y):
            x += w + COL_GAP
        return float(round(x)), float(round(y))
    left = min(n["pos"]["x"] for n in nodes.values())
    bottom = max(n["pos"]["y"] + n["h"] for n in nodes.values())
    return float(round(left)), float(round(bottom + ROW_GAP))


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def layout_process_group(ctx: Context, group_id: str, dry_run: bool = False) -> dict:
        """Tidy a process group's canvas: arrange components top-to-bottom in flow order with no overlaps,
        straighten connections, and route loops around the side.

        Thin labels (header bars) are treated as section titles: each section is laid out on its own,
        sections keep their arrangement, and each header moves to sit above its section. Taller labels
        (notes) stay where they are. dry_run returns the plan without moving anything.
        """
        return await apply_layout(_nifi(ctx), group_id, dry_run)


async def apply_layout(nifi: NiFiClient, group_id: str, dry_run: bool = False) -> dict:
    """The body of layout_process_group, callable from other tools."""
    nodes, edges, flow = await _nodes_and_edges(nifi, group_id)
    labels = flow["flow"].get("labels", [])
    positions, bends, label_updates = plan_sectioned_layout(nodes, edges, labels)
    moved = [n for n, p in positions.items() if (p["x"], p["y"]) != (round(nodes[n]["pos"]["x"]), round(nodes[n]["pos"]["y"]))]
    if dry_run:
        return {"group_id": group_id, "would_move": len(moved),
                "positions": {nodes[n]["name"]: positions[n] for n in moved},
                "section_headers": {next(l["component"]["label"] for l in labels if l["id"] == lid)[:60]: u
                                    for lid, u in label_updates.items()}}
    for n in moved:
        node = nodes[n]
        body: dict[str, Any] = {"revision": {"clientId": CLIENT_ID, "version": node["revision"]["version"]},
                                "component": {"id": n, "position": positions[n]}}
        await nifi.request("PUT", f"/{PATHS[node['kind']]}/{n}", json=body)
    current_labels = {l["id"]: l for l in labels}
    label_updates = {lid: u for lid, u in label_updates.items()
                     if (u["x"], u["y"], u["width"]) != (round(current_labels[lid]["position"]["x"]),
                                                       round(current_labels[lid]["position"]["y"]),
                                                       round(current_labels[lid]["component"]["width"]))}
    for lid, u in label_updates.items():
        fresh = await nifi.get(f"/labels/{lid}")
        await nifi.request("PUT", f"/labels/{lid}", json={
            "revision": {"clientId": CLIENT_ID, "version": fresh["revision"]["version"]},
            "component": {"id": lid, "position": {"x": u["x"], "y": u["y"]}, "width": u["width"],
                          "height": fresh["component"]["height"]}})
    conns = {c["id"]: c for _, _, c in edges}
    rebent = 0
    for cid, b in bends.items():
        if conns[cid]["component"].get("bends", []) != b:
            fresh = await nifi.get(f"/connections/{cid}")
            await nifi.request("PUT", f"/connections/{cid}", json={
                "revision": {"clientId": CLIENT_ID, "version": fresh["revision"]["version"]},
                "component": {"id": cid, "bends": b, "labelIndex": 0 if b else None}})
            rebent += 1
    return {"group_id": group_id, "components": len(nodes), "moved": len(moved),
            "section_headers_moved": len(label_updates), "connections_rerouted": rebent}
