# 星火不熄接入适配清单

固定上游：`3366d569557c4fda3fd153040632de65e255396d`。
来源与原文件哈希见 `UPSTREAM.json`、`SOURCE_SHA256.json`。保留原 MIT 许可证。

上游源码保持原样，接入代码位于本项目 `src/xinghuo/veria/`，不运行上游 CLI、poller、CTFd 提交或 provider fallback。

## 可追溯复用

- `backend/loop_detect.py` 原逻辑复制到 `xinghuo.veria.loop_detect`，保留工具重复检测的 12/3/5 窗口；`LOOP_WARNING_MESSAGE` 逐字保留。
- `backend/prompts.py`、Claude solver sandbox preamble 和 coordinator 原文保存在 `xinghuo/veria/resources/*.txt`，运行时校验哈希，原样抽取并执行上游纯构造函数；不只是保存供查阅。
- `xinghuo.veria.prompts` 使用完整上游 Claude SDK 基线，保留段落、条件分支、示例与强调语气，进行逐项登记的最小冲突替换后追加星火附录。详见 `docs/VERIA_PROMPT_RESTORATION.md` 与实际生成的差异表。节省 token、工具漏装、保留大意都不是删减理由。
- `xinghuo.veria.driver` 保留 Claude Agent SDK 的 Bash 主工具、Pre/PostToolUse 边界、每五次工具调用检查广播，以及同一 session 的自然 continuation。单成员只有一个 reader、串行 query；不同成员并行。

## 提示词冲突修改

1. 单题 coordinator 只适配整场选题/开题与单题范围的矛盾，保留全部读轨迹、定向指导、广播、不要放弃卡住 solver、多次 bump 后仍可能突破及 `Cost is not a concern` 原文。用户明确保留 coordinator 的 Bash 能力，作为本地新增差异登记。
2. 求解者收到正确回执后提交获取路径并继续剩余目标；只有 supervisor 的整题完成决定停止。无成果 continuation 与已有成果 continuation 区分。
3. 工具架构、路径、Python 与安装状态按实际环境精确适配；安装范围取上游 Dockerfile、工具说明与提示词能力的并集。用户允许确实不可用的架构工具不安装，但必须记录原因，并只修改相应安装断言，完整保留方法与示例。
4. 原 `connection_info` 不再被无条件插入 `Run:`；完整题目与所有外层入口原样提供，由模型按 URL/TCP/说明解释。
5. 本地恢复上游 WebFetch/WebSearch，托管禁公网按实际条件局部说明；保留 XSS/SSRF 等方法与示例，托管回连要求可达的授权端点。普通文件读写经成员 Bash。
6. 增加简短共享目录/私有依赖/不可变发布路径和共享现场协调说明；不添加路线锁、固定专业角色或周期性 solver 报告任务。
7. reporter、全场 strategy 属于本项目新增角色并保留完整本地策略。reviewer 完整继承 solver 基线后追加复核职责，仅修改首步与有界结束条件；先逐已知路径复现，旧回执不算复现。影响共享现场时先提醒 coordinator 组织广播协调。

## 执行冲突修改

- 成员身份从模型名拆为固定 solver ID；每客户端 options/env 显式设置其模型和全部默认模型映射，不改全局环境。
- runtime 拥有每题共享工具容器，各成员独立 work/venv/cache；reviewer 每轮另有私有容器目录。角色停止只清其后代任务，保留容器和文件。
- 独立 special command 经 shlex 和语法检查处理。混合命令拒绝；hook 异常明确 deny，不返回可落回宿主执行的空结果。
- 原生 Bash 不预先加入 SDK `allowed_tools`，采用 `dontAsk`；只有成功的 PreToolUse 返回 allow 才准许重写命令。容器 helper 还检查可撤销全局 dispatch gate 和按 generation/member/command 登记的许可，避免准备停止期间已经拿到 SDK allow 的命令迟到启动；恢复退休的旧命令许可保持撤销。
- 原生文件与 Agent/Task 路径保持成员执行边界；web 按本地/托管真实条件配置；提交与通知仅调用当前 runtime 注入的接口。只运行 Claude Agent SDK 路径，无其他 provider/CTFd 回退。
- 取消 `_confirmed`、首个成功终止和模型最终输出直接确认路径；平台回执、去重、错误冷却与整题完成由 runtime/state 管理。
- 早期 session ID、持久会话目录与 SDK 进程身份登记；角色生命周期不依赖最终 ResultMessage 才保存。SDK 独立进程组、容器任务 generation/member/command 标记分别负责清理。
- pending bumps、消息、候选、reporter 截点和 reviewer 状态由 runtime/state 持久化。driver 不另造重试循环或内存覆盖式 bump 槽。

离线 mock 测试不证明 DeepSeek 兼容接口、实际模型长会话或真实靶场效果；这些验证未在本轮执行。
