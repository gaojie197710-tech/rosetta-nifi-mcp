# rosetta-nifi-mcp — 本库约定

- 代码、注释、标识符英文；README、`docs/**` 中文。
- Python 包名 `rosetta_nifi_mcp`，命令 `rosetta-nifi-mcp`。环境变量 `NIFI_*`/`NIFI_MCP_*`、证书身份 `CN=nifi-mcp`、
  `~/.nifi-mcp` 目录是已部署实例的运行契约，不随品牌改名。
- 写工具只在 `NIFI_MCP_ALLOW_WRITES=true` 时注册；新增写工具同样受此开关约束。
- 测试：`scripts/smoke_test.py` 与 `scripts/e2e_*.py`，对本地 NiFi 实跑（`uv run python scripts/<名>.py`）。
- 不改 `flow-tests/**` 的 fixture 让测试变绿。
