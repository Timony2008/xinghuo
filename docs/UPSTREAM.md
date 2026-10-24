# 固定上游与完整性

上游：[verialabs/ctf-agent](https://github.com/verialabs/ctf-agent)，固定提交 `3366d569557c4fda3fd153040632de65e255396d`。

`vendor/veria-ctf-agent/` 保留所选 37 个原文件及 SOURCE_SHA256.json、UPSTREAM.json；原许可证为 MIT。运行时通过 `src/xinghuo/veria/upstream_baseline.py` 校验打包的原始资源，并抽取完整提示词基线。不要直接跟随上游 main，也不要在 vendor 中做本地改写。

本地适配集中在 `src/xinghuo/veria/prompts.py`：只对角色范围、平台多 flag 完成条件、环境事实、已配置轮转等具体冲突做可审计替换；项目新增职责追加。Reviewer 继承完整 Solver 解题基线；Reporter 是本项目的独立本地角色，保留其完整基线。

```sh
uv run python scripts/audit_veria_upstream.py --output dist/prompt-audit
```

默认只核对本地固定文件并导出系统提示词、续轮全文、哈希和逐段变更台账。显式添加 `--fetch-upstream` 才会访问上游固定提交的源码归档，仍不调用模型或平台。工具安装清单及锁定来源位于 `docker/veria/`，不能以提示词声明或安装入口存在替代功能验收。
