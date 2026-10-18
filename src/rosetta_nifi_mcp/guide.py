"""Flow design guide: the practices a flow built through this server should follow, by topic.

Proven on Auto Quote v2 (2026-10-16). build_flow's http_stage template, stack and grid layouts implement most
of it, and check_flow_design reports where an existing flow departs from it.
"""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

TOPICS: dict[str, str] = {
    "structure": """\
One top-level group per business flow, holding numbered child groups by role:
10 Intake, 20 Pipeline (one child group per business stage), 30 Outcome & Notifications, 80 Ledger,
90 Dead Letters & Alerts. Settings live in one parameter context bound to every group (build_flow binds it
to each child; a group created through the API does not inherit it). Business differences between partners
belong in data (lookup files, parameters, master data), not in extra branches on the canvas.""",

    "stage_contract": """\
Every business stage is a process group with the same ports, so stages can be reordered, tested alone and
versioned on their own:
- in: the business object (content) and its attributes.
- success: the object as this stage changed it, to the next stage.
- rejected: a business "no", with error.code and error.msg.
- failure: a technical problem after in-stage retries, with error.code and error.msg.
- ledger: a copy at stage entry, for the status ledger.
The stage entry clears the previous stage's results (error.*, invokehttp.*, the response attributes) and
stamps <prefix>.stage, <prefix>.state=running and the event time <prefix>.at.
build_flow's GroupSpec.stage (template http_stage) generates exactly this for a stage that calls one HTTP API.""",

    "errors": """\
Decide rejected versus failure in one place per stage:
- rejected: HTTP 400, 404, 409, 422, or a code listed as a business error.
- failure: 401, 403, 408, 429, 5xx, timeouts and connection errors. Retry these first (3 attempts, a few
  seconds apart), then route to failure.
Every technical failure also goes to dead letters (a file plus an ERROR bulletin). A user hears about each
outcome once. Never auto-terminate a relationship that can carry the business object, except a documented
business filter (say why in the processor's comments).""",

    "ledger": """\
Keep an explicit status ledger: one row per stage entry and per final outcome (key, event id, stage, state,
outcome, error code and message, business ids). It answers "where is my X" and "how many failed today"
without reading NiFi queues or provenance.
Stamp the event time where the event happens (UpdateAttribute with ${now():format(...,'UTC')}), never at
insert time: rows from different stages reach the ledger out of order, so insert time misorders them.
Ledger writes are a side path: retry, then report with an ERROR bulletin; never block the business flow.""",

    "intake": """\
Intake from Dossier (Open Atlas): a Dossier operation target calls the flow over MCP JSON-RPC.
execute_mutation delivers {:quote/payload "<json>" :quote/key "<delivery id>"}; execute_eql asks for the
receipt by key. The flow answers both from the ledger's latest row for the key, so a redelivered key is
answered, never processed twice. Every receipt, including the immediate "accepted" and "unknown" replies,
must carry every field the target reads: state (submitted/accepted/running/completed/failed/unknown), the
event id, the external id path (e.g. cpo_id, empty until known) and the time path (e.g. at). A missing
field makes Dossier record the delivery as unknown.""",

    "gotchas": """\
NiFi behaviours that silently break a flow built through the API (check_flow_design flags them):
- A child group created through the API does not inherit its parent's parameter context: bind each group.
- A parameter inside a quoted Expression Language literal is not substituted: ${x:matches('#{CODES}')}
  compares against the text "#{CODES}". Copy the parameter into an attribute in an UpdateAttribute value
  (cfg.codes = #{CODES}) and use ${x:matches(${cfg.codes})}.
- InvokeHTTP penalizes FlowFiles it routes to Retry with its own penalty (default 30 sec), on top of any
  RetryFlowFile penalty. Set the InvokeHTTP penalty explicitly (e.g. 5 sec).
- RetryFlowFile keeps its counter in an attribute; give each retry loop its own attribute name and clear it
  at stage entry, or a later loop starts with an exhausted count.""",

    "layout": """\
Layout rules (build_flow layout="stack" and layout="grid" apply them):
- Top-level groups in a grid about 1,000 apart horizontally and 700 vertically, so connection labels sit in
  the gaps instead of over the groups.
- Several connections between the same two components each get their own bend point, staggered, or NiFi
  draws them as one line ("overlapping connections").
- Inside a pipeline group, stack the stage groups top to bottom; run rejected / failure / ledger lines into
  shared vertical lanes (one bend per stage) with the output ports in a bottom row; side logic (a wait loop)
  sits beside the lanes and its long lines go around the bottom, not across the stages.
- After building, check_flow_design must report no overlapping connections.""",

    "testing": """\
Test the way the caller calls:
- Replace systems you cannot reach locally with a small simulator: master data in CSV files re-read on each
  request, and a faults file that injects an HTTP status per business id and endpoint.
- A scenario runner drives the flow through its real entry point (for Dossier: execute_mutation, then poll
  execute_eql) and checks the final receipt per scenario: happy path, redelivery not reprocessed, each
  business rejection, 5xx (technical failure + dead letter), 401 (technical, not rejected), transient 503s
  that recover, wait loops that resolve and that give up, unknown key.
- Rebuild from the builder (never hand-edit the canvas) and rerun every scenario before calling it done.""",
}


def guide(topic: str | None = None) -> dict:
    if topic is None:
        return {"topics": list(TOPICS), "guide": TOPICS}
    if topic not in TOPICS:
        raise ToolError(f"Unknown topic {topic!r}; topics: {', '.join(TOPICS)}")
    return {"topic": topic, "text": TOPICS[topic], "other_topics": [t for t in TOPICS if t != topic]}


def register(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def get_design_guide(topic: str | None = None) -> dict:
        """How flows built through this server should be designed: structure, stage contract, errors, ledger,
        intake from Dossier, NiFi gotchas, layout and testing. Read it before designing or building a flow.
        topic narrows to one section; omit it for all of them."""
        return guide(topic)
