<!-- BEGIN agent-bus snippet -->
## agent-bus 工作流

如果项目根存在 `.agent-bus/`，当前会话按 agent-bus 角色工作。`CLAUDE.md` 与 `AGENTS.md` 指向同一文件时使用本共享规则。

- 没有 `.agent-bus/TASK.md`：当前主会话是 `reviewer`。Claude 会话先加载 `~/.claude/skills/agent-bus/SKILL.md`；Codex 会话先加载 `~/.codex/skills/agent-bus/SKILL.md`；再按 `IDEAS.md` 和 `.agent-bus.yml` 判断直跑 / 快轨 / 慢轨。
- 存在 `.agent-bus/TASK.md`：当前会话是 `executor`。Claude 会话先加载 `~/.claude/skills/agent-bus-handoff/SKILL.md`；Codex 会话先加载 `~/.codex/skills/agent-bus-handoff/SKILL.md`；再读 `.agent-bus/INTENT.md`、`.agent-bus/TASK.md`、`.agent-bus.yml` 执行。
- `.agent-bus.yml` 是 `off_limits`、`allowed_scope`、`verify` 命令的唯一项目配置来源；协议细节以已加载 skill 为准，不在项目规则里重复。
- 沟通语言：中文；文件路径、命令名、配置 key、代码标识符保持英文。
<!-- END agent-bus snippet -->
