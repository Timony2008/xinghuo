# 一起开发

## 本地验证

```sh
uv sync --frozen --extra runtime
uv run --frozen --extra runtime python -m unittest discover -s tests -q
uv run --frozen --extra runtime python scripts/audit_veria_upstream.py --output dist/prompt-audit
```

真实 SDK 的离线协议验证需要独立 Python 3.14，但不需要真实 key：

```sh
uv venv --python 3.14 .venv-veria
uv pip install --python .venv-veria/bin/python -r requirements-veria.lock
uv pip install --python .venv-veria/bin/python -e .
.venv-veria/bin/python scripts/verify_evidence_sdk_wire.py --output dist/evidence-sdk-wire.json
.venv-veria/bin/python scripts/verify_mixed4_sdk_wire.py --output dist/mixed4-sdk-wire.json
```

这些脚本连接本机合成接口。Docker 工具验收、外部模型鉴权、真实平台测试是不同层级，报告结果时分别说明。运行付费模型或平台操作前，确认采用的是明确授权的配置与靶场。

## 分支与 PR

从 `main` 拉分支，围绕一个具体行为修改并提交 PR。PR 说明问题、最终行为、测试结果与未验证项；新增逻辑使用能发现真实回归的测试。需要变更依赖时同步更新 `uv.lock`。

修改提示词遵循“完整上游基线 → 明确冲突的最小替换 → 新增附录”。不要直接编辑 `vendor/` 中的固定上游原文件；人数、平台完成条件或轮转规则的冲突通过提示词构造器中的可审计补丁解决。

保留候选立即提交、平台权威回执、多 flag、原截止时间恢复和 reviewer 独立复现的边界。历史获取材料不能充当本次 reviewer 的新执行证据。native 和 Veria 的报告流程分别维护。

## 提交范围

不提交 `.env`、真实 API key、平台 token、完整运行目录、赛事附件、个人用量、会话记录、数据库、镜像或工具二进制。公开示例使用占位符与合成数据。历史本机说明与运行验证留在本地；通用设计结论请整理到公开开发文档。

仓库的 `.gitignore` 对 `docs/` 使用白名单；新增公开文档时同时更新白名单。共享材料先检查其内容，再显式加入版本控制。
