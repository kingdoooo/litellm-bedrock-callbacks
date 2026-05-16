# Ping Injector — Per-Client Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `callbacks/_route.py` 从二分类（Responses vs 非 Responses）升级成三分类端点分类器，让 `ping_injector` 不再向 OpenAI 兼容 `/v1/chat/completions` 客户端注入 `event: ping` 命名事件（这会让按行 JSON.parse 的客户端解析失败）。

**Architecture:** 用 `request_data["proxy_server_request"]["url"]` 的路径后缀做端点分类（LiteLLM 在 `litellm_pre_call_utils.py:1028` 写入完整 URL，比基于 body 形状的判别更可靠，因为 Anthropic Messages 与 Chat Completions body 都有 `messages` 字段）。`ping_injector` 改成显式三路：Anthropic Messages → 命名事件；Responses → SSE 注释；其它 → pass-through。`chunk_delayer` 同步改为仅在 Anthropic Messages 路径上施加延迟。

**Tech Stack:** Python 3.13（LiteLLM 容器内）/ asyncio / LiteLLM `CustomLogger` / docker-compose。

**Spec:** `docs/superpowers/specs/2026-05-16-ping-injector-client-routing-design.md`

**验证策略（沿用 ping-injector v1 plan 约定，不写 pytest 单测）：** 容器内 curl 抓帧 + 报错客户端复测。

**前置约束：**
- 本 repo 只追踪 `callbacks/` 目录、`docs/`、`README.md`；`docker-compose.yml`、`config.yaml`、`.env` 都在 `.gitignore` 中。
- 当前工作目录：`/home/ec2-user/litellm`
- 容器在运行：`docker compose ps` 应显示 `litellm` + `litellm-db`。
- volume 是 `./callbacks:/app/callbacks:ro` 只读挂载，改完源码必须 `docker compose restart litellm`，不能热加载。

---

## File Structure

| 文件 | 操作 | 职责 |
|-|-|-|
| `callbacks/_route.py` | 重写 | 导出 4 个端点常量 + `classify_endpoint(request_data) -> str` |
| `callbacks/ping_injector.py` | 修改 | 把单行 `frame = _COMMENT_FRAME if is_responses_api(...) else _PING_FRAME` 替换为三路分支，非 CC/Codex 直接 pass-through |
| `callbacks/chunk_delayer.py` | 修改 | `if is_responses_api(...)` 反向判定改成 `if classify_endpoint(...) != ANTHROPIC_MESSAGES` |

三个文件改动是一次原子重构（`_route.py` 导出符号变了），必须同一 commit 提交，否则中间态 `ImportError`。

---

## Task 1: 重写 `_route.py` 为三分类分类器，同步更新两个消费者

**Files:**
- Modify (rewrite): `callbacks/_route.py`
- Modify: `callbacks/ping_injector.py:21,40`
- Modify: `callbacks/chunk_delayer.py:20,34`

- [ ] **Step 1: 重写 `callbacks/_route.py`**

完整新内容：

```python
"""
Endpoint classifier for LiteLLM streaming-iterator callbacks.

LiteLLM stamps the full incoming URL into
`request_data["proxy_server_request"]["url"]` (see
litellm/proxy/litellm_pre_call_utils.py:1028) before any streaming hook
runs. We classify by URL path suffix instead of by body shape because
Anthropic Messages and OpenAI Chat Completions both carry a `messages`
field — body shape alone cannot distinguish them.

`endswith` on the path is intentional: LiteLLM also exposes prefixed
mounts like `/anthropic/v1/messages`, and a suffix match keeps the
classifier robust to that.

Leading underscore marks this module as internal — it is not a
registered callback dotted path.
"""

ANTHROPIC_MESSAGES = "anthropic_messages"
CHAT_COMPLETIONS = "chat_completions"
RESPONSES = "responses"
OTHER = "other"


def classify_endpoint(request_data: dict) -> str:
    psr = request_data.get("proxy_server_request") if isinstance(request_data, dict) else None
    url = (psr or {}).get("url") or ""
    if url.endswith("/v1/responses"):
        return RESPONSES
    if url.endswith("/v1/messages"):
        return ANTHROPIC_MESSAGES
    if url.endswith("/v1/chat/completions"):
        return CHAT_COMPLETIONS
    return OTHER
```

旧的 `is_responses_api` 删除——本 repo 内只有 `ping_injector` 与 `chunk_delayer` 引用，下面两步同步更新；LiteLLM 上游不导入它。

- [ ] **Step 2: 更新 `callbacks/ping_injector.py`**

把 import 行（当前 line 21）从：

```python
from callbacks._route import is_responses_api
```

改为：

```python
from callbacks._route import (
    ANTHROPIC_MESSAGES,
    RESPONSES,
    classify_endpoint,
)
```

把方法体里的 frame 选择行（当前 line 40）：

```python
        frame = _COMMENT_FRAME if is_responses_api(request_data) else _PING_FRAME
```

替换为：

```python
        endpoint = classify_endpoint(request_data)
        if endpoint == ANTHROPIC_MESSAGES:
            frame = _PING_FRAME
        elif endpoint == RESPONSES:
            frame = _COMMENT_FRAME
        else:
            # OpenAI-compatible Chat Completions and any other route:
            # do not inject. Their clients (e.g. line-based JSON.parse
            # consumers) cannot tolerate `event: ping` lines, and SSE
            # comments would not save them either. Pass through clean.
            async for chunk in response:
                yield chunk
            return
```

文件其它部分（`_PING_FRAME` / `_COMMENT_FRAME` 常量、`__init__`、pump/tick/main loop、`instance = PingInjector()`）不变。

- [ ] **Step 3: 更新 `callbacks/chunk_delayer.py`**

把 import 行（当前 line 20）从：

```python
from callbacks._route import is_responses_api
```

改为：

```python
from callbacks._route import ANTHROPIC_MESSAGES, classify_endpoint
```

把现有的 Responses-API short-circuit 块（当前 line 33-37）：

```python
        # Codex CLI / Responses API must never be delayed — this helper
        # exists only to synthesize slow TTFB for PingInjector tests on
        # the Anthropic Messages path.
        if is_responses_api(request_data):
            async for chunk in response:
                yield chunk
            return
```

替换为：

```python
        # ChunkDelayer exists only to synthesize a slow TTFB on the
        # Anthropic Messages path so PingInjector behavior can be
        # observed. Codex / Chat Completions / anything else must never
        # be delayed.
        if classify_endpoint(request_data) != ANTHROPIC_MESSAGES:
            async for chunk in response:
                yield chunk
            return
```

文件其它部分（`__init__` 读 env、`self.delay <= 0` short-circuit、first-chunk 延迟循环、`instance = ChunkDelayer()`）不变。

- [ ] **Step 4: 静态检查 import 是否成功**

容器内做 import-only smoke（不重启服务）：

```bash
docker exec litellm python -c "from callbacks._route import classify_endpoint, ANTHROPIC_MESSAGES, CHAT_COMPLETIONS, RESPONSES, OTHER; print('route ok'); from callbacks import ping_injector, chunk_delayer; print('callbacks ok')"
```

Expected:
```
route ok
callbacks ok
```

如果出现 `ImportError` 或 `ModuleNotFoundError`：检查文件路径是否在 `/home/ec2-user/litellm/callbacks/` 下；volume 是只读挂载，源码改动是直接落到容器里的（`./callbacks:/app/callbacks:ro`），不需要重建镜像。

- [ ] **Step 5: 单元级行为快速校验**

仍在容器内，验证四类输入返回正确标签：

```bash
docker exec litellm python -c "
from callbacks._route import classify_endpoint, ANTHROPIC_MESSAGES, CHAT_COMPLETIONS, RESPONSES, OTHER
def case(url):
    return classify_endpoint({'proxy_server_request': {'url': url}})
assert case('http://localhost:4000/v1/messages') == ANTHROPIC_MESSAGES
assert case('http://host:4000/anthropic/v1/messages') == ANTHROPIC_MESSAGES
assert case('http://localhost:4000/v1/chat/completions') == CHAT_COMPLETIONS
assert case('http://localhost:4000/v1/responses') == RESPONSES
assert case('http://localhost:4000/v1/embeddings') == OTHER
assert classify_endpoint({}) == OTHER
assert classify_endpoint({'proxy_server_request': None}) == OTHER
print('all classifier cases pass')
"
```

Expected:
```
all classifier cases pass
```

- [ ] **Step 6: Commit**

```bash
cd /home/ec2-user/litellm
git add callbacks/_route.py callbacks/ping_injector.py callbacks/chunk_delayer.py docs/superpowers/plans/2026-05-16-ping-injector-client-routing.md
git status
```

确认 `git status` 只显示这四个文件（`config.yaml` 等被 .gitignore 屏蔽）。然后：

```bash
git commit -m "$(cat <<'EOF'
fix(callbacks): three-way endpoint routing; pass-through Chat Completions

is_responses_api() conflated /v1/messages and /v1/chat/completions —
both carry a messages field — so OpenAI-compatible clients received
`event: ping` named events and choked when parsing SSE data lines as
JSON. Replace it with a URL-suffix classifier and a three-way switch:

  /v1/messages         (Claude Code)        -> event: ping (unchanged)
  /v1/responses        (Codex)              -> SSE comment (unchanged)
  /v1/chat/completions (OpenAI-compatible)  -> pass-through (NEW)
  anything else                              -> pass-through (NEW)

ChunkDelayer follows the same classifier and now delays only the
Anthropic Messages path so non-CC client streams are never perturbed
by the test harness.

Spec: docs/superpowers/specs/2026-05-16-ping-injector-client-routing-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: 端到端三端点 SSE 抓帧验证

**Files:** none (read-only verification)

**前提：** Task 1 已 commit。需要重启容器让 `PingInjector` / `ChunkDelayer` 实例重新拿到磁盘上的新代码（虽然挂载是只读 live mount，旧实例已存在于内存）。本 task 临时调小 ping 间隔、临时塞延迟，验证完恢复。

- [ ] **Step 1: 备份当前 `.env`**

```bash
cd /home/ec2-user/litellm
cp .env .env.bak.routing
grep -E '^(PING_INTERVAL_SECONDS|CHUNK_DELAY_SECONDS)=' .env || echo "(neither var currently set — restart will pick docker-compose defaults)"
```

- [ ] **Step 2: 把 ping 间隔调到 2s、首块延迟 5s（让 ping 帧在 5s TTFB 期间发 2 次）**

编辑 `.env`，确保包含：

```
PING_INTERVAL_SECONDS=2
CHUNK_DELAY_SECONDS=5
```

如果 `.env` 里已经有这两个键，就用 `sed` 改，没有则 append。然后：

```bash
docker compose up -d
docker compose ps
```

Expected: `litellm` 状态 `running` + `healthy`（或 `running`）。

- [ ] **Step 3: 取一个能 stream 的 master key**

```bash
grep '^LITELLM_MASTER_KEY=' /home/ec2-user/litellm/.env
```

后续用 `$KEY` 代指其值。在 shell 里 `export KEY=sk-...`（裸引号去掉）。

- [ ] **Step 4: 抓 `/v1/messages` 流（CC 路径，应有 `event: ping`）**

```bash
curl -sN -X POST http://localhost:4000/v1/messages \
  -H "x-api-key: $KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-haiku-4-5","stream":true,"max_tokens":50,"messages":[{"role":"user","content":"Say hi briefly."}]}' \
  | head -c 4000
```

Expected: 输出里在 `message_start` 之前能看到至少 1 个 `event: ping\ndata: {"type":"ping"}` 段（5s TTFB / 2s 间隔 ≈ 2 个 ping）。

- [ ] **Step 5: 抓 `/v1/responses` 流（Codex 路径，应有 SSE 注释 `:\n\n`）**

```bash
curl -sN -X POST http://localhost:4000/v1/responses \
  -H "Authorization: Bearer $KEY" \
  -H "content-type: application/json" \
  -d '{"model":"claude-haiku-4-5","stream":true,"max_output_tokens":50,"input":"Say hi briefly."}' \
  | head -c 4000 | cat -A | head -40
```

`cat -A` 把不可见字符显式化。Expected: 看到形如 `:$` 的孤立行——SSE 注释。**不**应看到 `event: ping`。

注意：CHUNK_DELAY_SECONDS 不影响 Responses 路径（Task 1 改后，chunk_delayer 仅延迟 Anthropic Messages），所以 Responses 不会被人为拖慢。如果 Bedrock TTFB 本来就 < 2s，可能恰好抓不到注释——把 max_output_tokens 调大、prompt 改长制造空闲，或临时把 PING_INTERVAL_SECONDS 调到 1s。

- [ ] **Step 6: 抓 `/v1/chat/completions` 流（验证 pass-through，绝不出现注入帧）**

```bash
curl -sN -X POST http://localhost:4000/v1/chat/completions \
  -H "Authorization: Bearer $KEY" \
  -H "content-type: application/json" \
  -d '{"model":"claude-haiku-4-5","stream":true,"max_tokens":50,"messages":[{"role":"user","content":"Say hi briefly."}]}' \
  > /tmp/cc_stream.txt
```

```bash
grep -c '^event: ping' /tmp/cc_stream.txt
grep -cE '^:$' /tmp/cc_stream.txt
head -c 2000 /tmp/cc_stream.txt
```

Expected:
- 第一个 grep 输出 `0`
- 第二个 grep 输出 `0`
- head 显示标准 OpenAI Chat Completions SSE：每行 `data: {"id":"chatcmpl-...","object":"chat.completion.chunk",...}`，最后 `data: [DONE]`

如果两个 grep 任意一个 > 0：Task 1 的实现有问题，回到 `ping_injector.py` 检查三路分支。

- [ ] **Step 7: 复测原报错的 OpenAI 兼容客户端**

用最初报 `JSON parsing failed: ... event: pin...` 的那个客户端再发一次相同请求，确认不再报错。

如果客户端没法立刻调出来，跳过本 step——Step 6 的 grep 已经确认 wire format 不再含 `event: ping`，等价证明。

- [ ] **Step 8: 还原 `.env` 测试旋钮**

```bash
cd /home/ec2-user/litellm
mv .env.bak.routing .env
docker compose up -d
docker compose ps
```

确认 `PING_INTERVAL_SECONDS` 与 `CHUNK_DELAY_SECONDS` 回到生产值（默认 `30` / `0`）。

- [ ] **Step 9: 不需要额外 commit**

`.env` / `docker-compose.yml` 不在 git 追踪范围（见前置约束）。Task 1 的 commit 已经包含全部代码 + plan 文档变更。

---

## 后续

Plan 全部步骤完成后，使用 `superpowers:finishing-a-development-branch` 决定如何把 Task 1 的 commit 推到 origin。
