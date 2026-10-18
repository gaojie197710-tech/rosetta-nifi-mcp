"""Group templates for build_flow. http_stage: one business stage that calls one HTTP API, following the
stage contract in the design guide (ports in / success / rejected / failure / ledger)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
INVOKE_HTTP = "org.apache.nifi.processors.standard.InvokeHTTP"
RETRY = "org.apache.nifi.processors.standard.RetryFlowFile"
ROUTE = "org.apache.nifi.processors.standard.RouteOnAttribute"


class StageSpec(BaseModel):
    template: Literal["http_stage"] = "http_stage"
    stage: str = Field(description="Stage id, written to <prefix>.stage and the ledger")
    url: str = Field(description="HTTP URL; parameters (#{...}) and Expression Language allowed")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    keep: dict[str, str] = Field(default_factory=dict, description=(
        "Attributes to set from a 2xx answer, e.g. {'aq.cpo_id': \"${stage.response:jsonPath('$.cpo_id')}\"}. "
        "${stage.response} is the body; response headers are ${'stage.h.<Header-Name>'}"))
    prefix: str = Field("flow", description="Attribute prefix for stage, state, outcome, at and the retry counter")
    header_pattern: str | None = Field(None, description=r"Attributes sent as request headers (regex); default '<prefix>\..*'")
    business_codes: str = Field("400|404|409|422", description="HTTP codes that mean a business 'no' (regex); may be #{PARAM}")
    error_code_header: str = "X-Error-Code"
    error_msg_header: str = "X-Error-Msg"
    retries: int = 3
    retry_penalty: str = "5 sec"
    read_timeout: str = "15 secs"
    ledger_port: bool = True


AT = "${now():format('yyyy-MM-dd HH:mm:ss.SSS', 'UTC')}"


def http_stage(s: StageSpec) -> dict:
    """Processors, ports, connections and fixed positions for an http_stage group."""
    p = s.prefix
    code_h, msg_h = f"'stage.h.{s.error_code_header}'", f"'stage.h.{s.error_msg_header}'"
    status = "invokehttp.status.code"
    processors = [
        {"name": "Enter stage", "type": UPDATE_ATTRIBUTE, "position": (0, 150),
         "comments": "Stage entry: clears the last stage's results, stamps the stage and its event time.",
         "properties": {
             f"{p}.stage": s.stage, f"{p}.state": "running", f"{p}.outcome": "in_progress", f"{p}.at": AT,
             # Unquoted here, so a #{PARAM} is substituted; the router then uses ${stage.business_codes}.
             "stage.business_codes": s.business_codes,
             "Delete Attributes Expression": rf"error\..*|invokehttp\..*|stage\.(response|h\..*)|{p}\.http\.retries.*"}},
        {"name": "Call API", "type": INVOKE_HTTP, "position": (0, 350), "auto_terminate": ["Response"],
         "penalty": s.retry_penalty, "comments": "Response body and headers land on the original FlowFile.",
         "properties": {
             "HTTP Method": s.method, "HTTP URL": s.url,
             "Request Header Attributes Pattern": s.header_pattern or rf"{p}\..*",
             "Request Content-Type": "application/json",
             "Request Body Enabled": "false" if s.method in ("GET", "DELETE") else "true",
             "Response Body Attribute Name": "stage.response", "Response Body Attribute Size": "4096",
             "Response Header Request Attributes Enabled": "true", "Response Header Request Attributes Prefix": "stage.h.",
             "Socket Read Timeout": s.read_timeout}},
        {"name": "HTTP retry", "type": RETRY, "position": (650, 350), "penalty": s.retry_penalty,
         "comments": f"5xx, timeout or connection error: {s.retries} attempts, {s.retry_penalty} apart.",
         "properties": {"Retry Attribute": f"{p}.http.retries", "Maximum Retries": str(s.retries),
                        "Penalize Retries": "true", "Reuse Mode": "reset"}},
        {"name": "Business or technical?", "type": ROUTE, "position": (-500, 550),
         "comments": "Only the business codes are a business 'no'; 401/403/408/429 are technical.",
         "properties": {"Routing Strategy": "Route to Property name",
                        "business": f"${{{status}:matches(${{stage.business_codes}})}}"}},
        {"name": "Business rejection", "type": UPDATE_ATTRIBUTE, "position": (-500, 750),
         "properties": {"error.code": f"${{{code_h}:isEmpty():ifElse(${{{status}:prepend('http_')}}, ${{{code_h}}})}}",
                        "error.msg": f"${{{msg_h}}}"}},
        {"name": "Technical failure", "type": UPDATE_ATTRIBUTE, "position": (650, 750),
         "properties": {
             "error.code": (f"${{{code_h}:isEmpty():ifElse(${{{status}:isEmpty():ifElse('connection_error', "
                            f"${{{status}:prepend('http_')}})}}, ${{{code_h}}})}}"),
             "error.msg": f"${{{msg_h}:isEmpty():ifElse(${{invokehttp.java.exception.message}}, ${{{msg_h}}})}}"}},
        {"name": "Keep result", "type": UPDATE_ATTRIBUTE, "position": (0, 750), "properties": dict(s.keep)},
    ]
    outputs = ["success", "rejected", "failure"] + (["ledger"] if s.ledger_port else [])
    # Columns at -500 / 0 / 650 leave room for a connection label (240 wide) between boxes; ports sit centred under
    # their processors. Output ports are 240 wide, processors 352.
    positions = {"in": (56, 0), "success": (56, 950), "rejected": (-444, 950), "failure": (706, 950), "ledger": (706, 190)}
    connections = [
        {"source": "in", "destination": "Enter stage"},
        {"source": "Enter stage", "destination": "Call API", "relationships": ["success"]},
        {"source": "Call API", "destination": "Keep result", "relationships": ["Original"]},
        {"source": "Call API", "destination": "HTTP retry", "relationships": ["Retry", "Failure"]},
        {"source": "Call API", "destination": "Business or technical?", "relationships": ["No Retry"]},
        {"source": "HTTP retry", "destination": "Call API", "relationships": ["retry"]},
        {"source": "HTTP retry", "destination": "Technical failure", "relationships": ["retries_exceeded", "failure"]},
        {"source": "Business or technical?", "destination": "Business rejection", "relationships": ["business"]},
        # Below the labels of Original and retries_exceeded, then down beside the retry line into the box.
        {"source": "Business or technical?", "destination": "Technical failure", "relationships": ["unmatched"],
         "bends": [(14, 670), (746, 670)]},
        {"source": "Business rejection", "destination": "rejected", "relationships": ["success"]},
        {"source": "Technical failure", "destination": "failure", "relationships": ["success"]},
        {"source": "Keep result", "destination": "success", "relationships": ["success"]},
    ]
    if s.ledger_port:
        connections.append({"source": "Enter stage", "destination": "ledger", "relationships": ["success"]})
    return {"processors": processors, "input_ports": ["in"], "output_ports": outputs, "connections": connections,
            "positions": positions}
