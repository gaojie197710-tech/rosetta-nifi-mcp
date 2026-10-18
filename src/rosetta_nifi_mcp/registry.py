"""Phase 3 tools: version control through a Flow Registry, and flow definition export/import."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from .build import CLIENT_ID, WRITE, _nifi
from .client import DEFAULT_DIR, NiFiClient, NiFiError

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=False)


async def _group_revision(nifi: NiFiClient, group_id: str) -> dict:
    pg = await nifi.get(f"/process-groups/{group_id}")
    return {"clientId": CLIENT_ID, "version": pg["revision"]["version"]}


async def _run_update_request(nifi: NiFiClient, create_path: str, body: dict, timeout: float = 120) -> dict:
    """Version change and revert are async requests: create, poll until complete, delete."""
    import asyncio

    created = (await nifi.post(create_path, body))["request"]
    poll = created["uri"].split("/nifi-api", 1)[1]
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        req = created
        while not req.get("complete"):
            if asyncio.get_running_loop().time() > deadline:
                raise NiFiError(f"Request did not complete within {timeout}s (last state: {req.get('state')})")
            await asyncio.sleep(1)
            req = (await nifi.get(poll))["request"]
        if req.get("failureReason"):
            raise NiFiError(req["failureReason"])
        return req
    finally:
        try:
            await nifi.delete(poll)
        except NiFiError:
            pass


def _export_path(file_name: str, suffix: str = ".json") -> Path:
    """Resolve a bare file name inside the export directory; anything that escapes it is refused."""
    base = Path(os.environ.get("NIFI_MCP_EXPORT_DIR", str(DEFAULT_DIR / "exports"))).expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)
    path = (base / file_name).resolve()
    if path.parent != base or path.suffix != suffix:
        raise ToolError(f"file_name must be a plain name ending in {suffix}; files are saved in {base}")
    if path.exists():
        raise ToolError(f"{path} already exists; choose another name")
    return path


def _vci_summary(vci: dict | None) -> dict | None:
    if not vci:
        return None
    return {k: vci.get(k) for k in ("registryId", "registryName", "bucketId", "bucketName", "flowId", "flowName",
                                    "version", "state", "stateExplanation")}


def register_read(mcp: MCPServer) -> None:
    @mcp.tool(annotations=READ_ONLY)
    async def list_registry_flows(ctx: Context, registry_id: str | None = None, bucket_id: str | None = None) -> dict:
        """Browse flow registries: with no arguments lists registry clients; with registry_id lists its buckets;
        with both lists the flows in that bucket and their versions."""
        nifi = _nifi(ctx)
        if not registry_id:
            regs = (await nifi.get("/controller/registry-clients"))["registries"]
            return {"registries": [{"id": r["id"], "name": r["component"]["name"], "type": r["component"]["type"].rsplit(".", 1)[-1],
                                    "properties": {k: v for k, v in r["component"]["properties"].items()
                                                   if not any(s in k.lower() for s in ("token", "password", "secret", "key"))}}
                                   for r in regs]}
        if not bucket_id:
            buckets = (await nifi.get(f"/flow/registries/{registry_id}/buckets"))["buckets"]
            return {"buckets": [{"id": b["id"], "name": b["bucket"]["name"], "description": b["bucket"].get("description")} for b in buckets]}
        flows = (await nifi.get(f"/flow/registries/{registry_id}/buckets/{bucket_id}/flows"))["versionedFlows"]
        out = []
        for f in flows:
            vf = f["versionedFlow"]
            versions = (await nifi.get(f"/flow/registries/{registry_id}/buckets/{bucket_id}/flows/{vf['flowId']}/versions"))["versionedFlowSnapshotMetadataSet"]
            out.append({"flow_id": vf["flowId"], "name": vf["flowName"], "description": vf.get("description"),
                        "versions": [{"version": v["versionedFlowSnapshotMetadata"]["version"],
                                      "comments": v["versionedFlowSnapshotMetadata"].get("comments"),
                                      "author": v["versionedFlowSnapshotMetadata"].get("author"),
                                      "timestamp": v["versionedFlowSnapshotMetadata"].get("timestamp")} for v in versions]})
        return {"flows": out}

    @mcp.tool(annotations=READ_ONLY)
    async def get_version_status(ctx: Context, group_id: str) -> dict:
        """Version control state of a process group (UP_TO_DATE, LOCALLY_MODIFIED, STALE, SYNC_FAILURE...)
        and the list of local changes not yet committed."""
        nifi = _nifi(ctx)
        vci = (await nifi.get(f"/versions/process-groups/{group_id}")).get("versionControlInformation")
        if not vci:
            return {"group_id": group_id, "versioned": False}
        changes = []
        if vci.get("state") in ("LOCALLY_MODIFIED", "LOCALLY_MODIFIED_AND_STALE"):
            diffs = (await nifi.get(f"/process-groups/{group_id}/local-modifications"))["componentDifferences"]
            changes = [{"component": d.get("componentName"), "type": d.get("componentType"),
                        "differences": [x.get("difference") for x in d.get("differences", [])]} for d in diffs]
        return {"group_id": group_id, "versioned": True, **_vci_summary(vci), "local_changes": changes}

    @mcp.tool(annotations=READ_ONLY)
    async def export_flow(ctx: Context, group_id: str, file_name: str | None = None) -> dict:
        """Export a process group as a flow definition JSON (with referenced controller services).

        With file_name, saves it as a new file in the export directory (NIFI_MCP_EXPORT_DIR, default
        ~/.nifi-mcp/exports) and returns the path; otherwise returns the definition inline.
        """
        nifi = _nifi(ctx)
        resp = await nifi.request("GET", f"/process-groups/{group_id}/download", params={"includeReferencedServices": "true"})
        definition = resp.json()
        contents = definition.get("flowContents", {})
        summary = {"name": contents.get("name"), "processors": len(contents.get("processors", [])),
                   "process_groups": len(contents.get("processGroups", [])),
                   "controller_services": len(contents.get("controllerServices", [])),
                   "parameter_contexts": list(definition.get("parameterContexts", {}))}
        if file_name:
            path = _export_path(file_name)
            # O_EXCL: never overwrite an existing file.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as fh:
                json.dump(definition, fh, indent=2)
            return {"file": str(path), "bytes": path.stat().st_size, **summary}
        return {**summary, "definition": definition}


def register_write(mcp: MCPServer) -> None:
    @mcp.tool(annotations=WRITE)
    async def commit_version(
        ctx: Context, group_id: str, comment: str, registry_id: str | None = None,
        bucket_id: str | None = None, flow_name: str | None = None, require_tests_pass: bool = False,
        require_golden_pass_rate: float | None = None,
    ) -> dict:
        """Commit a process group to the Flow Registry.

        Already versioned: commits a new version of its flow (only comment needed). Not versioned yet:
        starts version control and needs registry_id and bucket_id; flow_name defaults to the group name.
        require_tests_pass runs the group's saved flow tests first and refuses the commit if any fail.
        require_golden_pass_rate (e.g. 99.5) replays the group's golden suites and refuses the commit if
        any suite's pass rate is below it; the evidence report is written to the export directory.
        """
        nifi = _nifi(ctx)
        if require_tests_pass:
            from .flowtests import run_tests

            report = await run_tests(nifi, group_id)
            if report["failed"]:
                raise ToolError(f"Commit refused: {report['failed']} of {report['cases']} flow tests failed: "
                                + json.dumps([r for r in report["results"] if not r["passed"]])[:3000])
        if require_golden_pass_rate is not None:
            from .golden import replay

            golden = await replay(nifi, group_id, threshold=require_golden_pass_rate, failure_samples=3)
            if not golden["suites"]:
                raise ToolError("Commit refused: require_golden_pass_rate is set but the group has no golden suites")
            if not golden["meets_threshold"]:
                rates = {s["suite"]: s["pass_rate"] for s in golden["suites"]}
                raise ToolError(f"Commit refused: golden pass rate below {require_golden_pass_rate}%: {rates}; "
                                f"report {golden.get('report_file')}; sample failures "
                                + json.dumps([f for s in golden["suites"] for f in s["failure_sample"]])[:2000])
        current = await nifi.get(f"/versions/process-groups/{group_id}")
        vci = current.get("versionControlInformation")
        if vci:
            if vci["state"] in ("STALE", "LOCALLY_MODIFIED_AND_STALE"):
                raise ToolError("A newer version exists in the registry; change_version to it before committing")
            if vci["state"] == "SYNC_FAILURE":
                raise ToolError(f"Group cannot reach its registry: {vci.get('stateExplanation')}")
            versioned_flow = {"registryId": vci["registryId"], "bucketId": vci["bucketId"], "flowId": vci["flowId"],
                              "flowName": vci["flowName"], "comments": comment, "action": "COMMIT"}
        else:
            if not (registry_id and bucket_id):
                raise ToolError("Group is not versioned yet: registry_id and bucket_id are required")
            name = flow_name or (await nifi.get(f"/process-groups/{group_id}"))["component"]["name"]
            versioned_flow = {"registryId": registry_id, "bucketId": bucket_id, "flowName": name,
                              "comments": comment, "action": "COMMIT"}
        result = await nifi.post(f"/versions/process-groups/{group_id}", {
            "processGroupRevision": await _group_revision(nifi, group_id), "versionedFlow": versioned_flow})
        return {"group_id": group_id, **_vci_summary(result["versionControlInformation"])}

    @mcp.tool(annotations=WRITE)
    async def import_from_registry(
        ctx: Context, parent_id: str, registry_id: str, bucket_id: str, flow_id: str,
        version: str | None = None, x: float | None = None, y: float | None = None,
    ) -> dict:
        """Instantiate a versioned flow from the registry as a new process group (latest version by default).
        This is how a flow is promoted: import (or change_version) the same flow on the target environment."""
        nifi = _nifi(ctx)
        if version is None:
            versions = (await nifi.get(f"/flow/registries/{registry_id}/buckets/{bucket_id}/flows/{flow_id}/versions"))["versionedFlowSnapshotMetadataSet"]
            version = str(max(int(v["versionedFlowSnapshotMetadata"]["version"]) for v in versions))
        if x is None or y is None:
            from .layout import free_position
            x, y = await free_position(nifi, parent_id, "process_group")
        pg = await nifi.post(f"/process-groups/{parent_id}/process-groups", {
            "revision": {"clientId": CLIENT_ID, "version": 0},
            "component": {"position": {"x": x, "y": y},
                          "versionControlInformation": {"registryId": registry_id, "bucketId": bucket_id,
                                                        "flowId": flow_id, "version": version}}})
        return {"id": pg["id"], "name": pg["component"]["name"], **_vci_summary(pg["component"].get("versionControlInformation"))}

    @mcp.tool(annotations=WRITE)
    async def change_version(ctx: Context, group_id: str, version: str | None = None) -> dict:
        """Upgrade or roll back a versioned process group to another registry version (latest by default).
        Running components are restarted by NiFi as part of the change."""
        nifi = _nifi(ctx)
        vci = (await nifi.get(f"/versions/process-groups/{group_id}")).get("versionControlInformation")
        if not vci:
            raise ToolError("Group is not under version control")
        if version is None:
            versions = (await nifi.get(f"/flow/registries/{vci['registryId']}/buckets/{vci['bucketId']}/flows/{vci['flowId']}/versions"))["versionedFlowSnapshotMetadataSet"]
            version = str(max(int(v["versionedFlowSnapshotMetadata"]["version"]) for v in versions))
        target = {k: vci[k] for k in ("registryId", "bucketId", "flowId")} | {"groupId": group_id, "version": version}
        await _run_update_request(nifi, f"/versions/update-requests/process-groups/{group_id}", {
            "processGroupRevision": await _group_revision(nifi, group_id), "versionControlInformation": target})
        after = (await nifi.get(f"/versions/process-groups/{group_id}"))["versionControlInformation"]
        return {"group_id": group_id, **_vci_summary(after)}

    @mcp.tool(annotations=DESTRUCTIVE)
    async def revert_local_changes(ctx: Context, group_id: str) -> dict:
        """Discard uncommitted changes in a versioned process group, back to its registry version."""
        nifi = _nifi(ctx)
        vci = (await nifi.get(f"/versions/process-groups/{group_id}")).get("versionControlInformation")
        if not vci:
            raise ToolError("Group is not under version control")
        await _run_update_request(nifi, f"/versions/revert-requests/process-groups/{group_id}", {
            "processGroupRevision": await _group_revision(nifi, group_id), "versionControlInformation": vci})
        after = (await nifi.get(f"/versions/process-groups/{group_id}"))["versionControlInformation"]
        return {"group_id": group_id, **_vci_summary(after)}

    @mcp.tool(annotations=WRITE)
    async def import_flow_definition(
        ctx: Context, parent_id: str, file_path: str, group_name: str | None = None,
        x: float | None = None, y: float | None = None,
    ) -> dict:
        """Create a process group from a flow definition JSON file (as produced by export_flow or the NiFi UI).

        Building a whole flow as one definition and importing it is often more reliable than many small calls.
        """
        nifi = _nifi(ctx)
        path = Path(file_path).expanduser()
        if not path.is_file():
            raise ToolError(f"No such file: {path}")
        definition: dict[str, Any] = json.loads(path.read_text())
        name = group_name or definition.get("flowContents", {}).get("name") or path.stem
        if x is None or y is None:
            from .layout import free_position
            x, y = await free_position(nifi, parent_id, "process_group")
        resp = await nifi.request("POST", f"/process-groups/{parent_id}/process-groups/upload", data={
            "groupName": name, "positionX": str(x), "positionY": str(y), "clientId": CLIENT_ID,
            "disconnectedNodeAcknowledged": "false"}, files={"file": (path.name, path.read_bytes(), "application/json")})
        pg = resp.json()
        return {"id": pg["id"], "name": pg["component"]["name"], "parent_id": parent_id,
                "invalid": pg.get("invalidCount"), "stopped": pg.get("stoppedCount")}
