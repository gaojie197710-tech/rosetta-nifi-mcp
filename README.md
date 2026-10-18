---
title: "Rosetta NiFi MCP"
description: "Describe a data flow in plain words; your AI assistant builds it in Apache NiFi 2.x, tests it, and ships it."
---

# Rosetta NiFi MCP

**Describe a data flow in plain words. Your AI assistant builds it in Apache NiFi, runs test data through it,
and puts it under version control.**

> The Rosetta Stone carried one text in three scripts, and a lost language became readable again.
> Rosetta NiFi MCP turns the NiFi canvas into something an AI can read, change and verify.

[中文版](README.zh-CN.md) · [License: Elastic 2.0](LICENSE) · Apache NiFi 2.x · Python 3.12+

![Rosetta NiFi MCP demo: a flow described in one sentence, built with one build_flow call, running in NiFi](docs/media/demo.gif)

<sub>[Watch as MP4](docs/media/demo.mp4)</sub>

---

## Why

NiFi is powerful, but building a flow means a lot of clicking: dozens of processors, property names that have
to be exactly right, connections, parameter contexts and retry settings. Debugging means opening queue after
queue to find the stuck FlowFile.

Rosetta NiFi MCP is a [Model Context Protocol](https://modelcontextprotocol.io) server that gives Claude (or
any MCP client) 44 tools for NiFi. With it, an AI assistant can:

- **Build a whole flow in one call.** `build_flow` takes a declarative spec. Before anything is created, every
  property name, relationship and connection is checked against NiFi's own processor schemas.
- **Test the flow.** It can inject FlowFiles, read outputs, diff them against expectations, replay thousands of
  golden files, and evaluate Expression Language, RecordPath and Jolt inside NiFi itself.
- **Debug it.** `diagnose` reports what is wrong with a processor or group in one compact answer: validation
  errors, bulletins, back pressure and recent provenance.
- **Ship it.** It can commit to the NiFi Registry, block the commit while tests fail, and promote the flow from
  dev to prod.
- **Keep it readable.** It lays out the canvas automatically, places section headers, and checks the design
  against a built-in style guide.

You stay in charge: the server is **read-only** unless you set `NIFI_MCP_ALLOW_WRITES=true`.

## Quick start

```bash
git clone https://github.com/gaojie197710-tech/rosetta-nifi-mcp.git
cd rosetta-nifi-mcp
NIFI_HOME=/path/to/nifi-2.x scripts/setup-nifi-mcp-cert.sh   # client cert for a dev NiFi
uv sync
```

Add the server to Claude Code (`.mcp.json` is already in the repo) or to any MCP client:

```json
{
  "mcpServers": {
    "nifi": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/rosetta-nifi-mcp", "rosetta-nifi-mcp"],
      "env": { "NIFI_URL": "https://localhost:9443", "NIFI_MCP_ALLOW_WRITES": "true" }
    }
  }
}
```

Then just ask.

## Examples: generating workflows with MCP

### 1. From a sentence to a running flow

> **You:** Build a flow that accepts JSON orders over HTTP on port 8085, adds a `received.at` timestamp,
> drops orders without an `id`, and writes the rest to `/data/orders`. Put the port and folder in a
> parameter context.

The assistant reads `get_design_guide`, looks up the processor schemas, and sends one `build_flow` call:

```json
{
  "name": "Order intake",
  "parameter_context": {
    "name": "order-intake",
    "parameters": [
      {"name": "listen.port", "value": "8085"},
      {"name": "out.dir", "value": "/data/orders"}
    ]
  },
  "processors": [
    {"name": "receive orders", "type": "org.apache.nifi.processors.standard.ListenHTTP",
     "properties": {"Listening Port": "#{listen.port}", "Base Path": "orders"}},
    {"name": "stamp", "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
     "properties": {"received.at": "${now():format('yyyy-MM-dd HH:mm:ss')}"}},
    {"name": "has id?", "type": "org.apache.nifi.processors.standard.RouteOnContent",
     "properties": {"Match Requirement": "content must contain match", "has_id": "\"id\"\\s*:"},
     "auto_terminate": ["unmatched"],
     "comments": "Orders without an id are dropped on purpose."},
    {"name": "write", "type": "org.apache.nifi.processors.standard.PutFile",
     "properties": {"Directory": "#{out.dir}"}, "auto_terminate": ["success", "failure"],
     "comments": "End of the flow; failures stay visible as bulletins."}
  ],
  "connections": [
    {"source": "receive orders", "destination": "stamp", "relationships": ["success"]},
    {"source": "stamp", "destination": "has id?", "relationships": ["success"]},
    {"source": "has id?", "destination": "write", "relationships": ["has_id"]}
  ],
  "sections": [
    {"title": "Intake", "members": ["receive orders", "stamp"]},
    {"title": "Validate & store", "members": ["has id?", "write"]}
  ],
  "start": true
}
```

If a property name is wrong (for example `Listen Port` instead of `Listening Port`), nothing is created. The
call returns every problem in one list, the assistant fixes the spec and sends it again. The finished group
is laid out, has section headers, and comes back with design warnings from `check_flow_design`.

### 2. A multi-stage API pipeline from a template

> **You:** For each incoming record, call the enrichment API, then the pricing API. Retry 5xx three times,
> send 4xx business errors to a rejected lane, and keep a ledger copy at each stage.

Child groups with `stage` are generated from the built-in `http_stage` template. The template provides the
in / success / rejected / failure / ledger ports, retry with an explicit penalty, and the rejected-or-failure
decision:

```json
{
  "name": "Pricing pipeline", "layout": "stack", "input_ports": ["in"],
  "parameter_context": {"name": "pricing", "parameters": [
    {"name": "API", "value": "https://api.example.com"},
    {"name": "CODES", "value": "not_allowed,out_of_stock"}]},
  "groups": [
    {"name": "10 · Enrich", "stage": {"stage": "enrich", "prefix": "p", "url": "#{API}/enrich",
                                       "business_codes": "#{CODES}", "retry_penalty": "1 sec"}},
    {"name": "20 · Price",  "stage": {"stage": "price",  "prefix": "p", "url": "#{API}/price/${p.id}",
                                       "business_codes": "#{CODES}", "retry_penalty": "1 sec"}}
  ],
  "connections": [
    {"source": "in", "destination": "10 · Enrich/in"},
    {"source": "10 · Enrich/success", "destination": "20 · Price/in"}
  ]
}
```

The parameter context is bound to every nested group, because NiFi does not do that for groups created
through the API. The `stack` layout puts the rejected, failure and ledger lines into tidy vertical lanes.

### 3. Test it, then commit only when it passes

> **You:** Add a test: an order with id 42 must pass the id check unchanged, and an order without an id must
> not. Then commit to the registry, but only if the tests pass.

```yaml
# flow-tests/Order intake/orders.yaml
- name: order with id is stored
  input: {content: '{"id": 42}'}
  target: stamp
  output: {from: has id?, relationship: has_id}
  expect:
    content: {json_subset: {id: 42}}   # or exact: / contains:; attributes: {name: value, or null = absent}
```

`run_flow_tests` → all green → `commit_version(require_tests_pass=true)`. For bigger changes,
`run_golden_replay` sends thousands of recorded input/output pairs through the flow. It writes a pass-rate
report with the NiFi version, the flow version and a SHA-256 of the suite, which you can keep as release
evidence. `commit_version(require_golden_pass_rate=99.5)` blocks the commit if a suite falls below that rate.

### 4. "Why is nothing coming out?"

> **You:** The pricing pipeline stopped producing output. What's wrong?

`diagnose` on the group returns only the problem components: an invalid processor with its validation error,
the queue that is back-pressured at 100%, the last provenance events and a preview of their content. All of
it fits in a few hundred tokens, even for a group with 2,000 processors. Then the assistant can try a fix
safely: `test_expression` evaluates the corrected EL or Jolt inside NiFi before touching the real flow.

### 5. Explain an inherited flow

> **You:** Describe what `Legacy ETL` actually does and save it as documentation.

`describe_flow` reads the real configuration: inputs, steps in flow order with their non-default settings,
outputs, external endpoints, controller services and parameters. With `markdown_file` it also writes the
description as Markdown. `health_check` adds a summary for the whole canvas, covering invalid components,
port conflicts, registry drift, disk and heap, and days left on each certificate.

## Tools at a glance

| Area | Tools |
|---|---|
| Explore | `get_flow`, `get_processor`, `search_flow`, `search_processor_types`, `get_processor_schema`, `get_bulletins`, `list_queue`, `get_flowfile`, `search_provenance` |
| Understand | `describe_flow`, `diagnose`, `health_check`, `get_design_guide`, `check_flow_design` |
| Build | `build_flow`, `create_process_group`, `create_processor`, `update_processor`, `connect`, `create_funnel`, controller services, parameter contexts, `layout_process_group` |
| Test | `inject_test_flowfile`, `run_flow_tests`, `run_golden_replay`, `test_expression` |
| Ship | `list_registry_flows`, `get_version_status`, `commit_version`, `change_version`, `revert_local_changes`, `import_from_registry`, `export_flow`, `import_flow_definition` |

Large flows stay cheap. Listing tools return counts and summaries plus at most `limit` entries, worst first,
with filters by status, name and type and a paging hint for the next page.

The full reference is in the [中文版 README](README.zh-CN.md#工具).

## Configuration

| Variable | Default |
|---|---|
| `NIFI_URL` | `https://localhost:9443` |
| `NIFI_CLIENT_CERT` / `NIFI_CLIENT_KEY` | `~/.nifi-mcp/client.crt` / `client.key` |
| `NIFI_CA_CERT` | `~/.nifi-mcp/nifi-server-chain.pem` |
| `NIFI_MCP_ALLOW_WRITES` | unset: read-only |
| `NIFI_MCP_EXPORT_DIR` | `~/.nifi-mcp/exports` (never overwritten) |
| `NIFI_MCP_FLOW_TESTS_DIR` | `flow-tests/` |
| `NIFI_REGISTRY_URL` | `http://localhost:18080` (tests only) |

## Safety

- Read-only by default. Write tools are registered only when you opt in.
- Sensitive parameter values are write-only and never returned. No password or key is ever printed.
- `build_flow` validates the whole spec first. If creation fails partway, the new group and parameter
  context are removed.
- Test helpers clean up after themselves. Temporary listeners use random ports and secret paths, and never
  port 18080.

## Testing

End-to-end scripts run against a real NiFi:

```bash
uv run python scripts/smoke_test.py            # every read-only tool
uv run python scripts/e2e_build_flow_test.py   # build_flow + parameters, data in and out
```

See `scripts/e2e_*.py` for the rest: nested pipelines, golden replay, health check, large flows and the registry.

## License

[Elastic License 2.0](LICENSE).

---

Built by Jack & Eric Gao · No. 9527
