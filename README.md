# 星火不熄 · Xinghuo

面向 CTF 与授权测试靶场的 Python 多智能体解题框架。外层 supervisor 管理题目生命周期、平台提交、时间预算与恢复；单题团队使用固定版本的 Veria 工作流。

当前源码版本为 **0.4.4**。本仓库用于共同开发，包含源码、测试、工具镜像构建清单和固定上游源码；运行记录、真实凭证、赛事附件及预构建镜像不在仓库中。

## 开始开发

需要 Git、[uv](https://docs.astral.sh/uv/) 和 Python 3.12。执行：

```sh
git clone https://github.com/Timony2008/xinghuo.git
cd xinghuo
uv sync --frozen --extra runtime
uv run --frozen --extra runtime python -m unittest discover -s tests -q
uv run --frozen --extra runtime python -m xinghuo demo --duration 5
```

`demo` 使用确定性的 native 假角色与假平台，不调用付费模型或真实靶场。运行产物保存在被 Git 忽略的 `runs/`。完整工具链、Docker 和独立 Python 3.14 不属于上述纯源码测试的前置条件；环境相关用例会说明跳过原因。

## 运行方式

- **native**：负责人、动态 worker、reporter 与 reviewer；适合本地开发和通信流程验证。
- **veria**：固定 solver、coordinator、reporter 与按需 reviewer，采用 Claude Agent SDK。完整解题提示词来自固定上游，项目职责作为附录加入。
- **hosted**：单容器自动入口，使用平台注入的凭证；工具直接运行于外层容器，成员拥有各自工作目录。

托管默认配置为 `flash4`：每题四个 `deepseek-flash` solver，支持角色也使用 Flash，最多三题并发，默认总预算八小时。每题累计工作 15 分钟没有新增平台确认的正确 flag 时轮转；有新 flag 重新计时。`mixed4` 与 `legacy6` 仍保留，用于明确选择或按旧配置恢复。通用本地 Config 的默认值与托管默认值不同，请显式选择配置。

启动真实运行前，复制配置模板并填写本机路径，按部署环境提供模型及平台凭证。`config.veria.example.toml` 中的解释器路径必须替换为本机独立 Python 3.14 路径。私有配置使用 `config.local.toml` 等被忽略的文件名。

```sh
# 只准备完整 Veria 工具环境；需要 Docker、网络与足够磁盘空间。
bash scripts/prepare_veria.sh

# 填好私有 config.local.toml，并在进程环境中设置凭证后：
uv run --frozen --extra runtime python -m xinghuo run --config config.local.toml --run-dir runs/my-run
uv run --frozen --extra runtime python -m xinghuo status --run-dir runs/my-run
uv run --frozen --extra runtime python -m xinghuo resume --run-dir runs/my-run
```

新运行使用新目录；恢复沿用原截止时间、成员与 provider/model 绑定。程序不会自动读取任意 `.env` 文件，请由启动环境显式注入变量。`hosted.env.example` 是托管网关模板，其地址不适用于普通本地直连。

## 目录

| 路径 | 内容 |
| --- | --- |
| `src/xinghuo/` | supervisor、平台适配、角色运行时、证据和恢复 |
| `src/xinghuo/veria/` | Veria SDK、工具环境、完整提示词与持久通信 |
| `policies/` | native 角色及全场策略；包内也保留安装后的策略资源 |
| `tests/` | 单元测试与离线集成测试 |
| `scripts/` | 工具准备、提示词审计、SDK 合成端点和镜像验收 |
| `docker/veria/` | 工具版本、校验值与安装清单 |
| `vendor/veria-ctf-agent/` | 固定提交的上游原文件、哈希和许可证 |

## 协作与验证

请先阅读 [协作说明](CONTRIBUTING.md)、[架构概览](docs/ARCHITECTURE_OVERVIEW.md) 和 [当前边界](docs/DEVELOPMENT_STATUS.md)。修改提示词时运行：

```sh
uv run --frozen --extra runtime python scripts/audit_veria_upstream.py --output dist/prompt-audit
```

此命令默认离线，核对 37 个上游原文件并导出实际提示词差异，不调用模型或比赛平台。真实 SDK 合成端点检查使用 Python 3.14，见协作说明。

完整工具镜像可通过 `scripts/build_hosted.sh` 构建。构建和验收需要较大本机容量；上传限制、工具权限与外部模型连接必须分别验收。离线测试成功不表示真实网关、比赛得分或八小时稳定性已验证。

## 上游与许可

Veria 固定于 [`3366d569557c4fda3fd153040632de65e255396d`](https://github.com/verialabs/ctf-agent/tree/3366d569557c4fda3fd153040632de65e255396d)，原有 MIT 许可证和归属完整保留，详见 [上游说明](docs/UPSTREAM.md) 与 [第三方声明](THIRD_PARTY_NOTICES.md)。项目新增代码尚未单独指定开源许可证。
