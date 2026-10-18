---
title: "Rosetta NiFi MCP"
description: "Rosetta NiFi MCP（中文）：让 AI 助手探索、搭建、测试和发布 Apache NiFi 2.x 流程的 MCP 服务。"
---

# Rosetta NiFi MCP

> 罗塞塔石碑用同一段文字对照三种文字，让失传的语言重新可读。
> Rosetta NiFi MCP 把 NiFi 画布翻译成 AI 能读、能改、能验证的工具，让数据流说清楚自己在做什么。

Rosetta NiFi MCP 是 Apache NiFi 2.x 的 MCP 服务。第一阶段只读：画布浏览、处理器 schema、bulletin、队列检查和
provenance。第二阶段加入搭建、运行和测试流程的工具；第三阶段加入 Flow Registry 版本管理、流程定义导出导入和画布自动布局。
写工具只在 `NIFI_MCP_ALLOW_WRITES=true` 时注册。

## 布局

| 路径 | 内容 |
|---|---|
| `src/rosetta_nifi_mcp` | MCP 服务（`server.py` 注册全部工具） |
| `scripts/setup-*.sh` | 开发实例的证书与 SSO 准备脚本 |
| `scripts/smoke_test.py`、`scripts/e2e_*.py` | 对本地 NiFi 实跑的冒烟与端到端测试 |
| `flow-tests/` | 保存的流程测试用例与 golden 套件 |
| `release.json` | 版本与 provides/requires 区间 |

## 安装

1. `NIFI_HOME=<NiFi 安装目录> scripts/setup-nifi-mcp-cert.sh` 在 `~/.nifi-mcp` 生成客户端证书（`CN=nifi-mcp`），让 NiFi 信任它并授予管理员的策略。只用于开发实例。
2. `uv sync`
3. Claude Code 从 `.mcp.json` 加载服务（命令 `rosetta-nifi-mcp`）。
4. 可选：NiFi 经 Authelia 做 OIDC 单点登录时，`NIFI_HOME=… AUTHELIA_DATA=… scripts/setup-nifi-sso-secret.sh` 生成 client secret 并重启两边（`AUTHELIA_CONTAINER`、`AUTHELIA_URL` 可覆盖）。

## 测试

全部对本地 NiFi 实跑，`uv run python scripts/<名>.py`：

| 脚本 | 检查什么 |
|---|---|
| `smoke_test.py` | 调用每个只读工具 |
| `e2e_build_test.py` | 在临时组里搭流程、注入测试数据、检查输出、删除组 |
| `e2e_registry_test.py` | 版本管理、导出导入、布局，用 NiFi Registry 的临时 bucket（`NIFI_REGISTRY_URL`，默认 `http://localhost:18080`） |
| `e2e_build_flow_test.py` | `build_flow` 拒绝坏 spec；绑定新参数上下文搭流程，更新参数前后各跑一遍数据；清理组与上下文 |
| `e2e_diagnose_test.py` | `diagnose` 处理无效处理器和背压队列 |
| `e2e_flow_tests_test.py` | 搭 `flow-tests/mcp-flowtest-sandbox/` 的沙箱流程并运行，检查失败用例的 diff 和提交闸门 |
| `e2e_expression_test.py` | 经 `test_expression` 求值 EL、RecordPath、Jolt（含坏表达式），确认沙箱组被删除 |
| `e2e_describe_flow_test.py` | `describe_flow` 在 listener → file 沙箱上的输出与 Markdown |
| `e2e_health_check_test.py` | `health_check` 标出端口冲突（含 Registry 的 18080）、无效处理器、背压队列；证书到期（critical、warning、expired、ok）；跳过带密码的存储 |
| `e2e_large_flow_test.py` | 生成 400 个处理器的组，确认 `get_flow`、`diagnose`、`describe_flow`、`health_check` 在过滤和分页下保持紧凑 |
| `e2e_golden_replay_test.py` | 以 99.5% 回放示例 golden 套件和生成的 2,000 对套件，检查汇总、报告文件和提交闸门（约一分钟） |
| `e2e_nested_build_test.py` | 嵌套流程（顶层 grid，两个 `http_stage` 组堆叠，对本地测试 API）：拒绝坏的嵌套 spec、参数上下文挂在每个组上、设计检查干净；四个 FlowFile 分别走 success、rejected、failure（500 重试后）、failure（401） |

## 配置

| 变量 | 默认值 |
|---|---|
| `NIFI_URL` | `https://localhost:9443` |
| `NIFI_CLIENT_CERT` | `~/.nifi-mcp/client.crt` |
| `NIFI_CLIENT_KEY` | `~/.nifi-mcp/client.key` |
| `NIFI_CA_CERT` | `~/.nifi-mcp/nifi-server-chain.pem` |
| `NIFI_MCP_ALLOW_WRITES` | 未设置（只读） |
| `NIFI_MCP_EXPORT_DIR` | `~/.nifi-mcp/exports`（`export_flow` 存放文件处；从不覆盖） |
| `NIFI_MCP_FLOW_TESTS_DIR` | `flow-tests/` |
| `NIFI_MCP_EXTRA_CERTS` | 未设置（`health_check` 额外检查的 PEM 文件，`:` 分隔） |

## 工具

### 只读

`get_about`、`get_flow`、`get_processor`、`search_flow`、`search_processor_types`、`get_processor_schema`、
`get_bulletins`、`list_queue`、`get_flowfile`、`search_provenance`、`list_registry_flows`、`get_version_status`、
`export_flow`、`diagnose`、`describe_flow`、`health_check`、`get_design_guide`、`check_flow_design`。

**`get_design_guide`** 按主题返回经本服务搭建的流程应如何设计：结构、stage 契约、错误、台账、从 Dossier 接入、NiFi 坑点、布局和测试。
服务的 instructions 要求客户端在设计流程前先读它。

**`check_flow_design`** 按设计指南检查一个组（含子组）：

| 规则 | 发现什么 |
|---|---|
| `parameter_context_missing` | 组内处理器用了 `#{...}` 但组没有参数上下文。经 API 创建的组不继承父组的上下文。 |
| `parameter_in_quoted_el` | `#{...}` 写在带引号的 EL 字面量里，NiFi 不会替换。 |
| `invokehttp_retry_penalty` | InvokeHTTP 接 RetryFlowFile 却用默认 30 秒 penalty，每次重试都等 30 秒。 |
| `overlapping_connections` | 同一对组件、同一路由上的多条连接，NiFi 会画成一条线。 |
| `undocumented_auto_terminate` | 失败类关系被自动终止，却没有注释说明原因。 |

`health_check` 在 `design` 下汇总整张画布的这些警告。

**`health_check`** 汇总整张画布：每组的无效和停止组件、背压队列、错误 bulletin、版本管理状态（标出 SYNC_FAILURE 和本地漂移）、
registry client 可达性、JVM 堆和仓库磁盘用量，以及监听类处理器配置的端口——同一端口两个 listener、或占用 NiFi Registry 端口
（18080，或 registry client URL 中的端口）都会标出。它还报告以下证书的剩余天数：

- NiFi 服务端证书，从 TLS 握手读取，所以过期的也能报出；
- 本服务的客户端证书；
- 它的 CA bundle；
- `NIFI_MCP_EXTRA_CERTS` 列出的 PEM 文件。

少于 30 天为 warning，少于 7 天为 critical。需要密码的 keystore/truststore（PKCS12、JKS、NiFi 自身的存储）跳过，列在
`certificates.skipped`；SSL context service 引用的存储同样跳过：那些路径在 NiFi 主机上，这里从不读取。不读取也不打印任何密码。
`issues` 列出需要处理的事项。

**`describe_flow`** 按实际配置描述一个组：输入（listener 及其端口和路径、input port、生成器、无上游的组件）、按流向排列的步骤
及其非默认设置和路由、输出（文件、HTTP 调用与响应、output port）、外部端点、controller service 和参数上下文；子组嵌套描述。
给 `markdown_file`（一个普通 `.md` 文件名）时，同时把 Markdown 写进导出目录，从不覆盖。

**大流程。** `get_flow`、`diagnose`、`describe_flow`、`health_check` 返回计数和汇总，加上至多 `limit` 条（最差或最忙的在前），
并附 `paging` 说明给出 `next_offset` 和下钻提示。`get_flow` 和 `diagnose` 可按 `status` 过滤（invalid、stopped、running、
disabled、erroring、back_pressured）；`get_flow`、`diagnose`、`describe_flow` 还可按 `name`、`type` 子串过滤。输出大小取决于
`limit` 而不是组的大小，所以 2,000 个处理器的组默认也只有几千 token。`describe_flow` 的 Markdown 文件始终完整。

**`diagnose`** 接受处理器或进程组 id。对处理器返回运行状态、校验错误、近期 bulletin、进出队列及背压状态（80% 为 `NEAR`，
100% 为 `ACTIVE`）、相邻组件名、未连接的关系，以及最近的 provenance 事件和内容预览。对组只覆盖有问题的组件（无效、出错、
或被背压队列喂入）：计数、每个问题一行，前 `detail` 个（默认 3）给完整详情。

### 写（第二阶段）

`create_process_group`、`create_processor`、`update_processor`、`connect`、`create_funnel`、`create_controller_service`、
`update_controller_service`、`set_run_state`、`inject_test_flowfile`、`empty_queue`、`delete_component`。

测试循环：`inject_test_flowfile` → `set_run_state` → `list_queue` / `get_flowfile` → `get_bulletins` → `update_processor`，
重复到输出正确为止。`inject_test_flowfile` 让注入器保持禁用，启动整个组时不会再次触发。

### 写（测试）

`run_flow_tests`、`run_golden_replay`、`test_expression`。

**保存的流程测试。** 用例放在 `flow-tests/<组名>/`，YAML 或 JSON（每个文件一个用例或一个列表；`NIFI_MCP_FLOW_TESTS_DIR`
可指向别处）。`run_flow_tests(group_id, case?)` 把每个用例的输入注入目标，从输出队列读第一个 FlowFile，报告通过/失败及 diff。
组内停止的处理器在运行期间启动、结束后停止；每个输出队列在用例前后清空。`commit_version(require_tests_pass=true)`
在任何用例失败时拒绝提交。

```yaml
- name: order with id is checked
  input: {content: '{"order": 42}', attributes: {order.id: "42"}}
  target: tag                                # component name or id to inject into
  output: {from: wrap, relationship: success} # or a connection id/name, or a component fed by one queue
  expect:
    attributes: {order.checked: "true", order.id: "42", unwanted: null}  # null = must be absent
    content: {json_subset: {wrapped: {order: 42}}}  # or exact: / contains:
  timeout: 15
```

**Golden 回放（E1 回归）。** `run_golden_replay(group_id, suite?, threshold?)` 把一个目录的输入/期望输出对回放过组，按阈值报告
通过率、耗时和失败样本及 diff。完整的逐例报告写进导出目录（`golden-<group>-<time>.json`）作为发布证据，含 NiFi 版本、流程版本和
套件的 SHA-256。输入以 POST 发给一个临时 ListenHTTP（空闲端口，绝不用 18080），属性作为请求头，连到目标；结束后删除 listener。
输出按 `mcp.golden.case` 属性配对，每批至多 100 个（NiFi 队列列表一次最多返回这么多）。本地约 40 例/秒，10,000 例约 4 分钟。
ListenHTTP 无法只绑定 loopback，所以回放期间它在所有网卡上、随机端口、随机密钥路径下监听。若 listener 删不掉，结果在
`cleanup_error` 中说明。`commit_version(require_golden_pass_rate=99.5)` 在任何套件低于阈值时拒绝提交。

```
flow-tests/<group>/golden/<suite>/
  suite.yaml                 target, output, match (exact|json|json_subset|contains), threshold, batch_size, timeout
  inputs/NAME.*              content to send
  expected/NAME.*            expected output
  attributes/NAME.json       optional input attributes
  expected_attributes/NAME.json  optional attributes the output must carry
```

**`test_expression(expression, language, attributes?, content?)`** 在 NiFi 内部求值：对属性求 Expression Language（UpdateAttribute），
对示例 JSON 求 RecordPath（UpdateRecord，每条记录一个值），或对示例 JSON 跑 Jolt chain spec（JoltTransformJSON）。它在隐藏的
`[mcp] expression sandbox` 组中运行，调用时创建、结束后删除，返回 `{ok, result}` 或带 NiFi 校验/运行时消息的 `{ok: false, error}`。

### 写（第三阶段）

`commit_version`、`change_version`、`revert_local_changes`、`import_from_registry`、`import_flow_definition`、`layout_process_group`。

**跨环境推广：** 在 dev 上 `commit_version`，再在目标环境上 `import_from_registry`（首次）或 `change_version`（升级），两边指向同一个 registry。

**摆放：** `create_processor` 和 `create_funnel` 接受 `below_id`，放在上游组件下方；不给坐标时，新组件放进空位而不是 (0, 0)。
`layout_process_group` 把整个组自上而下无重叠排开。细长的 label 视为分区标题：每个分区单独布局，分区保持相对位置，标题移到分区上方，
进入下方分区的连接在其标题上方直角转弯。较高的 label（注释）不动。用 `dry_run` 预览。

### 写（参数）

`list_parameter_contexts`、`create_parameter_context`、`update_parameter_context`、`assign_parameter_context`、
`delete_parameter_context`。敏感值只写不读，从不返回。更新走 NiFi 的异步更新请求，会重启使用了被改参数的处理器。
更新中 `value: null` 的参数会被删除。

### `build_flow`

一次调用在新进程组内建出整条流程：处理器（类型、属性、自动终止、调度、penalty、注释）、funnel、输入输出端口、任意深度的子
`groups`、按组件名给出的连接、`sections`（成员上方的标题 label），以及新的 `parameter_context` 或已有的 `parameter_context_id`。

- **嵌套：** 连接端点是同组组件或 `<子组>/<端口>`。参数上下文绑定到每个组，因为 NiFi 不为经 API 创建的组继承上下文。
- **Controller service：** `controller_services` 建在新组内，属性中以 `@service:<name>` 引用，并启用。
- **模板：** 带 `stage` 的子组由 `http_stage` 模板生成，遵循 stage 契约：端口 in / success / rejected / failure / ledger，
  入口打标和事件时间，HTTP 重试带显式 penalty，rejected 或 failure 的判定。业务代码来自属性，所以 `#{PARAM}` 可用。
- **布局：** `auto`（分层，默认）、`stack`（stage 组自上而下；rejected、failure、ledger 线进入竖向通道，端口沿底部排列）、
  `grid`（组按 `columns` 成行，相距约 1,000 x 700，label 位置互不重叠）和 `manual`（`positions`）。同一对组件间的连接总有不同的
  折点。连接可带显式 `bends`、`name` 和 `back_pressure_count`。
- **检查：** 创建任何东西之前，每个属性名、关系和连接端点都按处理器 schema 校验，所有问题一次返回（把显示名当属性名会被指出）。
  `dry_run=true` 到此为止。创建中途失败时，删除新组和新上下文。
- **结果：** 附带新组的 `check_flow_design` 警告。`start=true` 同时启动。

## 许可

[Elastic License 2.0](LICENSE)。

---

Built by Jack & Eric Gao · No. 9527
