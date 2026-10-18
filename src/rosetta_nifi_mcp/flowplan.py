"""Layout planners for build_flow, pure functions over names (no NiFi calls).

stack: child groups stacked top to bottom in declared order (a pipeline of stages). Output ports fed by two
       or more stacked groups become lanes: each stage's line takes one bend into a shared vertical lane and
       the port sits at the lane's foot, so nothing crosses the stack. Other components (side logic such as a
       wait loop) are laid out as a block left of the lanes; their long lines go around the bottom.
grid:  child groups in rows (`columns` per row) about 1,000 x 700 apart, so connection labels sit in the
       gaps. Same-row hops are straight, longer same-row hops go over the top, diagonal hops turn once in
       the gap, and label positions are kept apart.
stagger: connections between the same two components get distinct bend points (NiFi draws identical
       routes as one line).
"""

from __future__ import annotations

from collections import defaultdict

from .layout import SIZE, plan_layout

STACK_STEP = 250          # vertical distance between stacked stage groups
LEFT_LANE = -150          # x of the first lane, left of the stack
RIGHT_LANE0 = 600         # x of the first right-hand lane
LANE_STEP = 280
SIDE_GAP = 480            # side block to left lane: labels and tracks of side lines fit in between
GRID_X, GRID_Y = 1000, 700
LABEL_W, LABEL_H = 260, 90  # room a connection label needs around its bend point

Point = tuple[float, float]


def size(kind: str) -> tuple[int, int]:
    return SIZE[kind]


def center(pos: Point, kind: str) -> Point:
    w, h = size(kind)
    return pos[0] + w / 2, pos[1] + h / 2


def stagger(routes: list[tuple[int, Point, Point]]) -> dict[int, list[Point]]:
    """routes: (connection index, source centre, destination centre) of connections drawn on the same line.
    Returns one bend per connection, spread perpendicular to the line around its midpoint."""
    if len(routes) < 2:
        return {}
    (_, (sx, sy), (dx, dy)) = routes[0]
    vx, vy = dx - sx, dy - sy
    length = max((vx * vx + vy * vy) ** 0.5, 1.0)
    px, py = -vy / length, vx / length
    step = LABEL_W if abs(vy) >= abs(vx) else LABEL_H + 20
    mx, my = (sx + dx) / 2, (sy + dy) / 2
    n = len(routes)
    return {idx: [(round(mx + px * (j - (n - 1) / 2) * step), round(my + py * (j - (n - 1) / 2) * step))]
            for j, (idx, _, _) in enumerate(routes)}


def stagger_parallel(conns: list[dict], centers: dict[str, Point], bends: dict[int, list[Point]]) -> None:
    """In place: connections between the same two endpoints with the same (or no) bends get distinct bends."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for c in conns:
        if c["src"] == c["dst"]:
            continue
        groups[(frozenset((c["src"], c["dst"])), tuple(bends.get(c["idx"], [])))].append(c)
    for (_, existing), cs in groups.items():
        if len(cs) < 2 or existing:
            continue
        routes = [(c["idx"], centers[c["src"]], centers[c["dst"]]) for c in cs]
        bends.update(stagger(routes))


def _side_block(items: dict[str, str], side: list[str], conns: list[dict]) -> dict[str, Point]:
    """Relative positions for the non-stacked components, using the general layered layout."""
    nodes = {n: {"id": n, "kind": items[n], "w": size(items[n])[0], "h": size(items[n])[1],
                 "pos": {"x": 0, "y": i * 200}} for i, n in enumerate(side)}
    edges = [(c["src"], c["dst"], {"id": str(c["idx"])}) for c in conns if c["src"] in nodes and c["dst"] in nodes]
    positions, _ = plan_layout(nodes, edges)
    return {n: (p["x"], p["y"]) for n, p in positions.items()}


def plan_stack(items: dict[str, str], stacked: list[str], inputs: list[str], outputs: list[str],
               conns: list[dict]) -> tuple[dict[str, Point], dict[int, list[Point]], dict[int, int]]:
    """items: name -> kind for every component of the group (child groups included); conns: dicts with idx,
    src and dst as drawn on this canvas (a child group's port is drawn as the child group)."""
    positions: dict[str, Point] = {}
    bends: dict[int, list[Point]] = {}
    labels: dict[int, int] = {}  # connection index -> bend index that carries its label (default 0)
    gw, gh = size("process_group")
    for i, g in enumerate(stacked):
        positions[g] = (0, i * STACK_STEP)
    bottom = len(stacked) * STACK_STEP + 150
    stack_x = gw / 2 - size("input_port")[0] / 2
    for k, p in enumerate(inputs):
        positions[p] = (stack_x + k * 300, -200)

    feeders: dict[str, set[str]] = defaultdict(set)
    side_only: dict[str, bool] = defaultdict(lambda: True)
    outgoing: set[str] = set()
    for c in conns:
        feeders[c["dst"]].add(c["src"])
        side_only[c["dst"]] &= bool(c.get("side"))
        outgoing.add(c["src"])
    first = stacked[0] if stacked else None
    # Heads: components that only feed the first stage (a listener, say) sit above it, like input ports.
    heads = [n for n in items if n not in positions and n not in stacked and n not in feeders
             and all(c["dst"] == first for c in conns if c["src"] == n) and n in outgoing]
    if heads:
        tallest = max(size(items[n])[1] for n in heads)
        x = gw / 2 - (sum(size(items[n])[0] for n in heads) + 120 * (len(heads) - 1)) / 2
        for n in heads:
            w, h = size(items[n])
            positions[n] = (round(x), -tallest - 120 + (tallest - h) / 2)
            x += w + 120
    sinks = [n for n in items if items[n] in ("output_port", "funnel") and n not in outgoing and n not in positions]
    lanes = [p for p in sinks if len(feeders[p] & set(stacked)) >= 2 and feeders[p] <= set(stacked)]
    lane_x: dict[str, float] = {}
    for k, p in enumerate(lanes):
        lane_x[p] = LEFT_LANE if k == 0 else RIGHT_LANE0 + (k - 1) * LANE_STEP
        positions[p] = (lane_x[p] - size(items[p])[0] / 2, bottom)
    last = stacked[-1] if stacked else None
    for p in sinks:
        if p not in positions and feeders[p] == {last} and not side_only[p]:
            positions[p] = (gw / 2 - size(items[p])[0] / 2, bottom)
    # A side exit of one stage (its own failure funnel, say): a column right of the stack and its lanes, one
    # row per sink; the line bends where its label sits, between the stack and the column.
    column_x = (max(lane_x.values()) + 140 + 270 if len(lanes) > 1 else gw + 300)
    per_stage: dict[str, int] = defaultdict(int)
    for p in sinks:
        if p in positions or len(feeders[p]) != 1 or not side_only[p] or next(iter(feeders[p])) not in stacked:
            continue
        g = next(iter(feeders[p]))
        cy = positions[g][1] + 24 + 70 * per_stage[g]
        per_stage[g] += 1
        positions[p] = (column_x, round(cy - size(items[p])[1] / 2))
        for c in conns:
            if c["src"] == g and c["dst"] == p:
                bends[c["idx"]] = [(column_x - 150, round(cy))]

    side = [n for n in items if n not in positions]
    if side:
        rel = _side_block(items, side, conns)
        right = max(x + size(items[n])[0] for n, (x, _) in rel.items())
        top = min(y for _, y in rel.values())
        anchor_rows = [positions[c["dst"]][1] for c in conns if c["src"] in side and c["dst"] in stacked]
        anchor_rows += [positions[c["src"]][1] for c in conns if c["dst"] in side and c["src"] in stacked]
        y0 = min(anchor_rows) if anchor_rows else 0
        x0 = (LEFT_LANE if lanes else 0) - SIDE_GAP - right
        for n, (x, y) in rel.items():
            positions[n] = (round(x + x0), round(y - top + y0))
    side_right = max((positions[n][0] + size(items[n])[0] for n in side), default=LEFT_LANE - SIDE_GAP)

    centers = {n: center(positions[n], items[n]) for n in items}
    port_y = bottom + size("output_port")[1] / 2
    around = 0
    for c in conns:
        s, d, idx = c["src"], c["dst"], c["idx"]
        if d in lane_x:
            k = lanes.index(d)
            if s in stacked:
                y = positions[s][1] + 60 + 50 * min(k, 3)
                if k == 0:
                    bends[idx] = [(lane_x[d], y)]
                else:  # level out just right of the stage, so the line passes under nearer lanes' labels
                    bends[idx] = [(gw + 20, y), (lane_x[d], y)]
                    labels[idx] = 1
            else:
                # From the side block, out through the empty gap between it and the left lane, then down: the
                # run out of the block takes the first clear row (the source's centre, or just above or below
                # it), so no line crosses another side box. Each line gets its own track in the gap, and its
                # label sits in the gap, clear of the lane labels.
                around += 1
                y = clear_row(s, side, positions, items, side_right + LABEL_W)
                pre = [] if y == round(centers[s][1]) else [(round(centers[s][0]), y)]
                gx = side_right + LABEL_W + 25 * around
                route = pre + [(side_right + LABEL_W / 2, y), (gx, y)]
                if lane_x[d] == LEFT_LANE:
                    bends[idx] = route + [(gx, round(port_y))]
                else:  # under the bottom row to a right-hand lane
                    low = bottom + 100 + 50 * around
                    bends[idx] = route + [(gx, low), (lane_x[d], low)]
                labels[idx] = len(pre)
        elif lanes and ((s in stacked and d in side) or (s in side and d in stacked)):
            # Between a stage and the side block: turn left of the left lane, so the label is not on the lane.
            stage = s if s in stacked else d
            offset = 20 if s in stacked else 150  # apart from the side block's own lines at its centre row
            bends[idx] = [(LEFT_LANE - 120, positions[stage][1] + offset)]
    stagger_parallel(conns, centers, bends)
    return positions, bends, labels


def clear_row(src: str, side: list[str], positions: dict[str, Point], items: dict[str, str], x_end: float) -> int:
    """A row for a line leaving src to the right that crosses no other side box before x_end."""
    x, y, w, h = *positions[src], *size(items[src])
    others = [(positions[n][0], positions[n][1], *size(items[n])) for n in side if n != src]

    def clear(row: float, x_from: float) -> bool:
        return not any(ox - 10 < x_end and x_from < ox + ow + 10 and oy - 10 < row < oy + oh + 10
                       for ox, oy, ow, oh in others)
    if clear(y + h / 2, x + w):
        return round(y + h / 2)
    for step in range(1, 6):
        for row in (y - 30 * step, y + h + 30 * step):
            if clear(row, x + w / 2):
                return round(row)
    return round(y + h / 2)


def plan_grid(items: dict[str, str], groups: list[str], conns: list[dict],
              columns: int = 3) -> tuple[dict[str, Point], dict[int, list[Point]], dict[int, int]]:
    positions: dict[str, Point] = {}
    cell: dict[str, tuple[int, int]] = {}
    for i, g in enumerate(groups):
        r, col = divmod(i, columns)
        cell[g] = (r, col)
        positions[g] = (col * GRID_X, r * GRID_Y)
    rest = [n for n in items if n not in positions]
    if rest:
        rel = _side_block(items, rest, conns)
        rows = (len(groups) + columns - 1) // columns
        top = min(y for _, y in rel.values())
        for n, (x, y) in rel.items():
            positions[n] = (x, y - top + rows * GRID_Y)
    centers = {n: center(positions[n], items[n]) for n in items}
    gw, gh = size("process_group")
    gap_x, gap_y = (GRID_X - gw) / 2, (GRID_Y - gh) / 2

    bends: dict[int, list[Point]] = {}
    over = defaultdict(int)
    for c in conns:
        s, d, idx = c["src"], c["dst"], c["idx"]
        if s not in cell or d not in cell or s == d:
            continue
        (r1, c1), (r2, c2) = cell[s], cell[d]
        if r1 == r2 and abs(c2 - c1) > 1:  # over the top of the groups in between
            over[r1] += 1
            y = r1 * GRID_Y - 250 - 50 * (over[r1] - 1)
            bends[idx] = [(round(centers[s][0]), y), (round(centers[d][0]), y)]
        elif r1 != r2 and c1 != c2:  # turn once in the gaps nearest the destination
            x = c2 * GRID_X - gap_x if c2 > c1 else (c2 + 1) * GRID_X - gap_x
            y = (max(r1, r2)) * GRID_Y - gap_y
            bends[idx] = [(round(x), round(y))]
    stagger_parallel(conns, centers, bends)

    # Keep labels (drawn at the first bend) apart.
    used: list[Point] = []
    for c in conns:
        pts = bends.get(c["idx"])
        if not pts or len(pts) != 1:
            continue
        x, y = pts[0]
        while any(abs(x - ux) < LABEL_W and abs(y - uy) < LABEL_H for ux, uy in used):
            x += LABEL_W + 10
        bends[c["idx"]] = [(x, y)]
        used.append((x, y))
    return positions, bends, {}
