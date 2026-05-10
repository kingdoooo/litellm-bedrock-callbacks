# LiteLLM SSE Ping Injector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 LiteLLM 网关上注入 Anthropic-风格 SSE ping 心跳帧（空闲 30s 时发一个 `{"type":"ping"}`），修复 CC + Bedrock 长输出 hang。

**Architecture:** 新增一个 `CustomLogger` 子类 `PingInjector`，通过 LiteLLM 的 `async_post_call_streaming_iterator_hook` 挂到所有流式响应路径。实现内部用两个 asyncio.Task：`pump` 从上游消费 chunk 入队、`tick` 周期性投 ping 候选；主生成器循环从队列取，维护 `last_yield` 时间戳，ping 候选到达时按"距上次 yield ≥ interval"语义决定是否下发。

**Tech Stack:** Python 3.13（LiteLLM 容器内）/ asyncio / LiteLLM `CustomLogger` / docker-compose。

**Spec:** `docs/superpowers/specs/2026-05-09-ping-injector-design.md`

**验证策略（不写 pytest 单测，已在 brainstorm 确认）：** curl 抓帧 + CC 长文件实测端到端。

**前置约束：**
- 本 repo 只追踪 `callbacks/` 目录下的新文件和本 plan 文档；`docker-compose.yml`、`config.yaml`、`.env` 都在 `.gitignore` 中，它们的改动不进入 git 历史。
- 当前工作目录：`/home/ec2-user/litellm`
- 容器已运行（`docker compose ps` 应显示 `litellm` + `litellm-db`）。

---

## File Structure

| 文件 | 操作 | 职责 |
|-|-|-|
| `callbacks/__init__.py` | 创建 | 空文件，使 `callbacks/` 成为 Python package（`callbacks.ping_injector` 可以被 import） |
| `callbacks/ping_injector.py` | 创建 | 定义 `PingInjector(CustomLogger)`，实现 `async_post_call_streaming_iterator_hook` |
| `docker-compose.yml` | 修改 | 追加 `environment:` 段（`PYTHONPATH`、`PING_INTERVAL_SECONDS`），追加 `./callbacks:/app/callbacks:ro` volume |
| `config.yaml` | 修改 | `litellm_settings` 下追加 `callbacks: callbacks.ping_injector.PingInjector` |

---

## Task 1: 创建 `callbacks/` Python package

**Files:**
- Create: `callbacks/__init__.py`

- [x] **Step 1: 创建目录和空 `__init__.py`**

```bash
mkdir -p /home/ec2-user/litellm/callbacks
: > /home/ec2-user/litellm/callbacks/__init__.py
```

- [x] **Step 2: 验证目录结构**

```bash
ls -la /home/ec2-user/litellm/callbacks
```
Expected:
```
total 0
drwxr-xr-x ... .
drwxr-xr-x ... ..
-rw-r--r-- ... __init__.py
```

- [x] **Step 3: 确认 `__init__.py` 被 git 追踪（确认 `.gitignore` 没屏蔽）**

```bash
cd /home/ec2-user/litellm && git status --short
```
Expected: `?? callbacks/__init__.py`（以及 `?? docs/superpowers/plans/2026-05-09-ping-injector.md` 如果 plan 已生成）

---

## Task 2: 实现 `PingInjector`

**Files:**
- Create: `callbacks/ping_injector.py`

- [x] **Step 1: 写入完整实现**

Create `/home/ec2-user/litellm/callbacks/ping_injector.py`:

```python
"""
LiteLLM CustomLogger that injects Anthropic-style SSE ping frames
during idle periods of streaming responses.

Design doc: docs/superpowers/specs/2026-05-09-ping-injector-design.md
"""
import asyncio
import os
import time

from litellm.integrations.custom_logger import CustomLogger


class PingInjector(CustomLogger):
    def __init__(self):
        super().__init__()
        self.interval = float(os.environ.get("PING_INTERVAL_SECONDS", "30"))

    async def async_post_call_streaming_iterator_hook(
        self, user_api_key_dict, response, request_data
    ):
        q: asyncio.Queue = asyncio.Queue()

        async def pump():
            try:
                async for chunk in response:
                    await q.put(("item", chunk))
            except Exception as exc:
                await q.put(("error", exc))
            else:
                await q.put(("done", None))

        async def tick():
            while True:
                await asyncio.sleep(self.interval)
                await q.put(("ping", {"type": "ping"}))

        pump_t = asyncio.create_task(pump())
        tick_t = asyncio.create_task(tick())
        last_yield = time.monotonic()
        try:
            while True:
                kind, v = await q.get()
                if kind == "done":
                    return
                if kind == "error":
                    raise v
                if kind == "ping":
                    if time.monotonic() - last_yield < self.interval:
                        continue
                yield v
                last_yield = time.monotonic()
        finally:
            tick_t.cancel()
            pump_t.cancel()
```

- [x] **Step 2: 语法检查**

```bash
python3 -m py_compile /home/ec2-user/litellm/callbacks/ping_injector.py && echo OK
```
Expected: `OK`

- [x] **Step 3: Commit**

```bash
cd /home/ec2-user/litellm
git add callbacks/__init__.py callbacks/ping_injector.py
git commit -m "$(cat <<'EOF'
feat: add PingInjector CustomLogger for SSE ping heartbeat

Implements PRD Phase 1 (method A): asyncio queue-based injection via
async_post_call_streaming_iterator_hook. Idle-activated semantic —
ping is emitted only when ≥ PING_INTERVAL_SECONDS has elapsed since
the last frame yielded to the client.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [x] **Step 4: 确认 commit 成功**

```bash
git log --oneline -3
```
Expected: 最新一条是 `feat: add PingInjector CustomLogger ...`

---

## Task 3: 更新 `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`（gitignored — 不会进入 git）

当前 `litellm` service（`docker-compose.yml:20-34`）：

```yaml
  litellm:
    image: ghcr.io/berriai/litellm:v1.83.3-stable.opus-4.7
    container_name: litellm
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    network_mode: host
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./ai_usage_chat.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/ai_usage_chat.py
      - ./endpoints.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/endpoints.py
    command: --config /app/config.yaml --port 4000
```

- [x] **Step 1: 在 `env_file:` 段之后插入 `environment:` 段**

`PING_INTERVAL_SECONDS` 使用 `${VAR:-default}` 语法，方便后续验证阶段临时降低间隔（Task 6）。

用 Edit 或手工编辑，把：

```yaml
    env_file:
      - .env
    network_mode: host
```

改为：

```yaml
    env_file:
      - .env
    environment:
      - PYTHONPATH=/app
      - PING_INTERVAL_SECONDS=${PING_INTERVAL_SECONDS:-30}
    network_mode: host
```

- [x] **Step 2: 在 `volumes:` 段的 `config.yaml` 行之后插入 callbacks mount**

把：

```yaml
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./ai_usage_chat.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/ai_usage_chat.py
```

改为：

```yaml
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./callbacks:/app/callbacks:ro
      - ./ai_usage_chat.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/ai_usage_chat.py
```

- [x] **Step 3: 验证 compose 文件语法**

```bash
cd /home/ec2-user/litellm && docker compose config > /dev/null && echo OK
```
Expected: `OK`（compose 解析成功，无 YAML 错误）

- [x] **Step 4: 预览最终渲染，确认新 key 都在**

```bash
docker compose config | grep -E "PYTHONPATH|PING_INTERVAL|callbacks:/app"
```
Expected 三行：
```
- PYTHONPATH=/app
- PING_INTERVAL_SECONDS=30
- /home/ec2-user/litellm/callbacks:/app/callbacks:ro
```

---

## Task 4: 更新 `config.yaml`

**Files:**
- Modify: `config.yaml`（gitignored — 不会进入 git）

当前 `litellm_settings` 段（`config.yaml:108-113`）：

```yaml
litellm_settings:
  request_timeout: 600
  set_verbose: false
  num_retries: 3
  modify_params: true
  drop_params: true
```

- [x] **Step 1: 在 `litellm_settings` 末尾追加 `callbacks` 行**

改为：

```yaml
litellm_settings:
  request_timeout: 600
  set_verbose: false
  num_retries: 3
  modify_params: true
  drop_params: true
  callbacks: callbacks.ping_injector.PingInjector
```

- [x] **Step 2: YAML 语法检查**

```bash
python3 -c "import yaml; yaml.safe_load(open('/home/ec2-user/litellm/config.yaml'))" && echo OK
```
Expected: `OK`

---

## Task 5: 部署 + 启动烟测

**Files:** 无改动

- [x] **Step 1: 重建容器**

```bash
cd /home/ec2-user/litellm && docker compose up -d --force-recreate litellm
```
Expected 最后输出：`Container litellm  Started`

- [x] **Step 2: 检查容器状态**

```bash
docker compose ps litellm
```
Expected: `STATUS` 列显示 `Up ... (healthy)` 或 `Up ... seconds`（无 Restarting / Exit）

- [x] **Step 3: 检查启动日志，确认 callback 加载、无 import 错误**

```bash
docker logs litellm 2>&1 | tail -80
```
Expected:
- 有 `LiteLLM Proxy` 启动完成日志（Uvicorn `Application startup complete`）
- **没有** `ImportError` / `ModuleNotFoundError` / `Traceback` 相关内容
- 可能出现但正常：关于 `callbacks.ping_injector.PingInjector` 被注册的 debug 日志（不一定有）

若看到 `ModuleNotFoundError: No module named 'callbacks'`：检查 `PYTHONPATH=/app` 是否生效、`./callbacks:/app/callbacks:ro` 是否挂载正确。

- [x] **Step 4: 确认 callback 模块在容器内可 import**

```bash
docker exec litellm python -c "from callbacks.ping_injector import PingInjector; print(PingInjector())"
```
Expected: `<callbacks.ping_injector.PingInjector object at 0x...>`

- [x] **Step 5: 确认实例读取了环境变量**

```bash
docker exec litellm python -c "from callbacks.ping_injector import PingInjector; p = PingInjector(); print('interval =', p.interval)"
```
Expected: `interval = 30.0`

---

## Task 6: 验证 §6.1 — Ping 帧格式合规

**目标：** 确认 LiteLLM 的 SSE 序列化层把我们 yield 的 `{"type":"ping"}` 下发成 `event: ping\ndata: {"type":"ping"}\n\n` 形式（或至少 `data: {"type":"ping"}\n\n`）。

**策略：** 临时把 `PING_INTERVAL_SECONDS` 降到 `2` 秒，发一个能生成≥5 秒输出的 prompt，观察帧间是否出现 `ping` 事件。

**Files:**
- Modify: `.env`（临时，gitignored — 验证后 revert）

- [x] **Step 1: 在 `.env` 里临时设置 `PING_INTERVAL_SECONDS=2`**

```bash
echo "PING_INTERVAL_SECONDS=2" >> /home/ec2-user/litellm/.env
cat /home/ec2-user/litellm/.env
```
Expected 末尾一行：`PING_INTERVAL_SECONDS=2`

- [x] **Step 2: 重建容器让环境变量生效**

```bash
cd /home/ec2-user/litellm && docker compose up -d --force-recreate litellm
docker exec litellm python -c "import os; print('env =', os.environ.get('PING_INTERVAL_SECONDS'))"
```
Expected: `env = 2`

- [x] **Step 3: 发起 curl 流式请求，抓帧**

```bash
source /home/ec2-user/litellm/.env && \
curl -sN -X POST http://localhost:4000/v1/messages \
  -H "anthropic-version: 2023-06-01" \
  -H "x-api-key: $LITELLM_MASTER_KEY" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-haiku-4-5",
    "stream": true,
    "max_tokens": 2000,
    "messages": [{"role":"user","content":"请用中文写一篇约 1500 字的短文，题目：清晨的海边。慢慢写，一段一段展开。"}]
  }' > /tmp/ping_capture.txt 2>&1
wc -l /tmp/ping_capture.txt
```
Expected: 行数在几十到几百之间（模型实际输出了内容）。

- [x] **Step 4: 验证 ping 帧出现且格式合规**

```bash
grep -c -E '^(event: ping|data: \{"type":"ping"\})' /tmp/ping_capture.txt
```
Expected: `≥ 1`（至少有一行匹配）

```bash
grep -B1 -A1 'data: {"type":"ping"}' /tmp/ping_capture.txt | head -20
```
Expected 看到以下两种之一：

- **理想格式（方案 A 完美）：**
  ```
  event: ping
  data: {"type":"ping"}
  ```
- **降级格式（需继续 Task 10 的 CC 端到端确认）：**
  ```
  data: {"type":"ping"}
  ```
  如果只有 `data:` 没有 `event: ping`，记录下来，在 Task 10 验证 CC 是否识别。如果 CC 不识别，按 spec §8 回滚，启动 PRD Phase 2（方案 B）。

- [x] **Step 5: 保存帧样本，便于 Task 7 对比**

```bash
cp /tmp/ping_capture.txt /tmp/ping_sample_idle.txt
echo "saved: /tmp/ping_sample_idle.txt"
```

---

## Task 7: 验证 §6.2 — 帧密集期不发冗余 ping

**目标：** 确认"距上次 yield ≥ interval"语义生效 —— 模型快速吐 token 时不出现 ping。

**前置：** Task 6 已经把 `PING_INTERVAL_SECONDS=2` 设好并生效。

- [x] **Step 1: 发起一个快速响应的 curl（短 prompt + 限制 max_tokens）**

```bash
source /home/ec2-user/litellm/.env && \
curl -sN -X POST http://localhost:4000/v1/messages \
  -H "anthropic-version: 2023-06-01" \
  -H "x-api-key: $LITELLM_MASTER_KEY" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-haiku-4-5",
    "stream": true,
    "max_tokens": 50,
    "messages": [{"role":"user","content":"Say hi."}]
  }' > /tmp/ping_capture_short.txt 2>&1
```

- [x] **Step 2: 验证无 ping 帧（或极少）**

```bash
grep -c 'data: {"type":"ping"}' /tmp/ping_capture_short.txt
```
Expected: `0`（短请求应在 2 秒内完成，不应触发 ping）
- 若结果 `≥ 1`，说明实现的空闲语义错了（可能改成了定时发送），需要回到 Task 2 复核代码。

- [x] **Step 3: 对比 Task 6 样本**

```bash
echo "idle-triggered pings:"
grep -c 'data: {"type":"ping"}' /tmp/ping_sample_idle.txt
echo "short-request pings (expected 0):"
grep -c 'data: {"type":"ping"}' /tmp/ping_capture_short.txt
```
Expected：
```
idle-triggered pings:
<N > 0>
short-request pings (expected 0):
0
```

---

## Task 8: 验证 §6.4 — 正常流事件顺序回归

**目标：** 确认 `message_start` / `content_block_*` / `message_stop` 事件序列完整、没被 ping 打断逻辑。

**前置：** `PING_INTERVAL_SECONDS=2` 仍在生效。

- [x] **Step 1: 提取 Task 6 样本中的事件名序列**

```bash
grep -E '^event:' /tmp/ping_sample_idle.txt | sort -u
```
Expected 至少包含（可能顺序不同）：
```
event: content_block_delta
event: content_block_start
event: content_block_stop
event: message_delta
event: message_start
event: message_stop
```
如果还出现 `event: ping` 就更好（说明完美格式）。

- [x] **Step 2: 确认 message_start / message_stop 各出现恰好 1 次**

```bash
echo "message_start count:"
grep -c '^event: message_start' /tmp/ping_sample_idle.txt
echo "message_stop count:"
grep -c '^event: message_stop' /tmp/ping_sample_idle.txt
```
Expected: 两条都是 `1`。

- [x] **Step 3: 确认 message_start 是第一个 event、message_stop 是最后一个 event**

```bash
grep -n '^event:' /tmp/ping_sample_idle.txt | sed -n '1p;$p'
```
Expected:
- 第 1 行 event 是 `event: message_start`
- 最后一行 event 是 `event: message_stop`

---

## Task 9: 验证 §6.5 — 错误透传

**目标：** 确认 Bedrock 的 4xx 错误原样透传到客户端，错误响应里不掺 ping。

**前置：** `PING_INTERVAL_SECONDS=2` 仍在生效。

- [x] **Step 1: 发一个故意错的请求（不存在的模型）**

```bash
source /home/ec2-user/litellm/.env && \
curl -sN -X POST http://localhost:4000/v1/messages \
  -H "anthropic-version: 2023-06-01" \
  -H "x-api-key: $LITELLM_MASTER_KEY" \
  -H "content-type: application/json" \
  -d '{
    "model": "nonexistent-model-xyz",
    "stream": true,
    "max_tokens": 100,
    "messages": [{"role":"user","content":"hi"}]
  }' > /tmp/ping_err.txt 2>&1
cat /tmp/ping_err.txt
```
Expected: 一段 JSON 错误响应（LiteLLM 返回 400/404 或类似），**不包含** `data: {"type":"ping"}`。

- [x] **Step 2: 验证错误响应里没有 ping**

```bash
grep -c 'data: {"type":"ping"}' /tmp/ping_err.txt
```
Expected: `0`

---

## Task 10: 恢复默认间隔 + CC 长文件端到端（§6.3）

**目标：** 把间隔恢复到 30s，用真实 Claude Code 场景验证原始 bug 已修。

**Files:**
- Modify: `.env`（revert PING_INTERVAL_SECONDS）

- [x] **Step 1: 把 `.env` 里 `PING_INTERVAL_SECONDS=2` 删掉**

```bash
cd /home/ec2-user/litellm
sed -i '/^PING_INTERVAL_SECONDS=/d' .env
cat .env
```
Expected：没有 `PING_INTERVAL_SECONDS=` 行了。

- [x] **Step 2: 重建容器，确认间隔回到默认 30**

```bash
docker compose up -d --force-recreate litellm
sleep 3
docker exec litellm python -c "import os; print('env =', os.environ.get('PING_INTERVAL_SECONDS'))"
```
Expected: `env = 30`（compose 文件里 `${PING_INTERVAL_SECONDS:-30}` 的 fallback 生效）

- [x] **Step 3: （手动）在一个新 Claude Code 会话里跑长文件写入场景**

触发 PRD §1.1 中描述的场景：让 CC 一次性写一个较长的文件（5~10 分钟单次输出）。例如：

> 帮我写一个 ~3000 行的 Python 脚本，实现一个简易的 KV 存储引擎，包含 B+ 树、WAL、快照、压缩、测试。一次性写入一个文件 `/tmp/kv_engine.py`，不要分批。

- [x] **Step 4: 观察 CC 行为**

期望观察到（Pass criteria）：
- CC **不 hang**，能完成写入
- Write 工具调用一次完成，没有 tool 重试、没有报 `max_tokens` 错误
- 从 LiteLLM access log（`docker logs litellm 2>&1 | grep /v1/messages`）看是一次 stream 请求，没有后续的 non-stream fallback
- 期间可能能看到容器内 Traceback（仅当 callback 报错才会有 —— 正常不应有）

- [x] **Step 5: 如果 Task 6 Step 4 里观察到的是"降级格式"（只有 `data:` 没有 `event: ping`），且本步 CC 依然 hang**

→ 触发 PRD Phase 2 条件。按 spec §8 Rollback 处理：

```bash
cd /home/ec2-user/litellm
# 手动从 config.yaml 删除 `callbacks: callbacks.ping_injector.PingInjector` 行
docker compose restart litellm
```

然后回到 brainstorm 阶段，开启 Phase 2（方案 B：前置 FastAPI 包装层）的新设计流程。**这条 Plan 到此结束。**

- [x] **Step 6: 如果 CC 正常完成，记录验收通过**

```bash
date -u +"%Y-%m-%dT%H:%M:%SZ" | tee /tmp/ping_injector_e2e_passed.txt
```

---

## Task 11: 收尾 —— 更新 spec 状态 + 最终 commit

**Files:**
- Modify: `docs/superpowers/specs/2026-05-09-ping-injector-design.md`

- [x] **Step 1: 把 spec 的"状态"字段从"待实现"改为"已实现"**

Edit `/home/ec2-user/litellm/docs/superpowers/specs/2026-05-09-ping-injector-design.md`:

把：
```
- **状态**：待实现
```
改为：
```
- **状态**：已实现（2026-05-09）
```

- [x] **Step 2: Commit**

```bash
cd /home/ec2-user/litellm
git add docs/superpowers/specs/2026-05-09-ping-injector-design.md
git commit -m "$(cat <<'EOF'
docs: mark ping injector spec as implemented

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [x] **Step 3: 查看最终 git 状态**

```bash
git log --oneline
git status
```
Expected:
- `git log --oneline` 有三条 commit：初始 spec commit、`feat: add PingInjector ...`、`docs: mark ping injector spec as implemented`
- `git status` 显示 `working tree clean`（`.env`、`config.yaml`、`docker-compose.yml` 的本地改动被 `.gitignore` 隐藏，不应出现在 status 里）

---

## Out-of-scope（本 Plan 不做）

- PRD Phase 2 方案 B（前置 FastAPI 包装层）—— 仅在 Task 10 Step 5 证明方案 A 失败时才启动，需单独的 brainstorm → spec → plan。
- 日志 / metrics / 审计 / 鉴权 —— YAGNI。
- 按模型或端点过滤 —— 已在 brainstorm 决定对所有流式请求无差别注入。
- pytest 单元测试 —— 已在 brainstorm 排除；验证通过 curl + CC 实测完成。
- 把 `config.yaml` / `docker-compose.yml` 纳入 git —— 已在 brainstorm 决定不追踪这些文件。
