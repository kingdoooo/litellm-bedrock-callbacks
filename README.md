# litellm-bedrock-callbacks

> 自部署 LiteLLM 网关的 **CustomLogger 扩展** / CustomLogger add-ons for a self-hosted LiteLLM gateway.
>
> 目标场景：把 AWS Bedrock 上的 Anthropic Claude（Opus / Sonnet / Haiku）接给不同风格的客户端，修掉几类代理层才能解决的兼容问题。
>
> Use case: bridge AWS Bedrock Anthropic Claude models to different client styles, patching the compatibility gaps that only a proxy layer can close.

---

## 支持的场景 / Supported scenarios

| 客户端 / Client | 接入协议 / Wire | 解决的问题 / Fixes | 关键回调 / Key callbacks |
|---|---|---|---|
| **Claude Code** | Anthropic Messages API (`POST /v1/messages`) | Bedrock 长空闲导致 CC 回退 non-stream 卡死 | `ping_injector` |
| **Codex CLI** | OpenAI Responses API (`POST /v1/responses`) | Codex 硬编码 `client_metadata` 被 Bedrock 拒；同样长空闲问题 | `codex_sanitizer` + `ping_injector` |

两类客户端可同时挂在同一个 LiteLLM 上，回调会按 `call_type` 和请求体自动分流，互不影响。

---

## 各问题/回调的作用 / Problem catalog

### 1. Bedrock 长空闲心跳（`ping_injector`）

**问题 / Problem**

Bedrock Converse 的 SSE 流在推理阶段会出现**长时间无帧**的空闲窗口（几十秒级）。

- Claude Code 空闲 ≥ 阈值后判定 stream 异常，**回退 non-stream** 并把 `max_tokens` 裁到 21333，长输出超时/截断，用户体感"卡死"
- Codex CLI `stream_idle_timeout_ms` 超时后直接断流

Anthropic 官方 API 的解决办法是在空闲期下发 `event: ping` 心跳帧 —— 只要客户端在空窗期仍收到字节，就不会误判。Bedrock 原生流没有这个心跳。

> 这是一个 **客户端 ↔ Bedrock 原生 SSE** 之间的兼容性问题，与 LiteLLM 本身无关。本 repo 只是因为我们的部署**恰好**使用 LiteLLM 作为网关，才顺势在它的扩展点上打补丁；换成任何在客户端和 Bedrock 中间的其他代理层，方案是一样的。

**作用 / How it works**

在 `async_post_call_streaming_iterator_hook` 挂一个包装器 `PingInjector`，位于"LiteLLM 拿到上游 SSE chunks"和"SSE 序列化器把帧写回客户端"之间 —— 这是改 SSE 行为最后一个干净的插入点。

实现上用一个 `asyncio.Queue` 把三件事解耦：

- **pump** 任务：消费上游 async 迭代器，把真实 chunk 塞进 queue
- **tick** 任务：每 `PING_INTERVAL_SECONDS` 秒往 queue 塞一个"心跳候选"
- **main loop**：从 queue 里拿东西往下 yield；心跳候选要通过"距上次 yield ≥ interval"的 idle-aware 检查，否则丢弃 —— 保证帧密集期不会插多余心跳

**按客户端选帧格式**：

- Anthropic 路径（body 含 `messages`）：发 `event: ping\ndata: {"type":"ping"}\n\n`
- Responses 路径（body 含 `input`）：发 SSE 注释帧 `:\n\n`（按 WHATWG SSE 规范被 Codex 的 `eventsource_stream` 在事件分发前消费掉，对解析器完全透明，但 socket 上有字节流动重置 idle timer）

不改 LiteLLM 源码、不引入新容器、不加新日志指标。

---

### 2. Codex 硬编码字段剥除（`codex_sanitizer`）

**问题 / Problem**

Codex Rust 源码 `codex-rs/core/src/client.rs:761` 硬编码在 Responses API 请求体顶层注入：

```json
"client_metadata": {"x-codex-installation-id": "<uuid>"}
```

这不是 OpenAI Responses API 的规范字段（`openai/openai-openapi` 仓库 0 命中），真 OpenAI 静默忽略，但 Bedrock Converse 严格校验直接拒绝：

```
BedrockException - {"message":"The model returned the following errors:
client_metadata: Extra inputs are not permitted"}
```

Codex 还可能带 `include: ["reasoning.encrypted_content"]`，Bedrock 同样不认。上游 issue [`openai/codex#17910`](https://github.com/openai/codex/issues/17910) 已 WONTFIX。

**LiteLLM 原生 `drop_params` / `additional_drop_params` 救不了** —— 这俩只对 `/v1/chat/completions` 生效，`/v1/responses` 路径不跑（社区 issue #20515 / #25931 / #19225）。

**作用 / How it works**

`CustomLogger.async_pre_call_hook` 是 LiteLLM 官方文档化的扩展点，参数里有 `call_type: CallTypesLiteral`。`CodexSanitizer` 只在 `call_type == "aresponses"` 时动作：

- `data.pop("client_metadata", None)`
- 过滤 `data["include"]` 里的 `"reasoning.encrypted_content"`

对 Claude Code（`call_type == "anthropic_messages"`）和 Chat Completions（`call_type == "acompletion"`）零影响。

---

### 3. Codex 端模型 metadata 识别（`docs/model_catalog.example.json`）

**问题 / Problem**

Codex 内部维护一个模型 metadata 表（context window、reasoning 能力、truncation 策略等），匹配规则是 `slug` 的 **longest-prefix match**。自定义 slug 命中不到时触发 warning：

```
⚠ Model metadata for `claude-opus-4-7` not found. Defaulting to fallback metadata;
  this can degrade performance and cause issues.
```

并且用 fallback 参数（context window 通常会被压到很小）。这是 Codex 端配置问题，不在 LiteLLM 侧处理。

**作用 / How it works**

提供一份示例 `model_catalog.json`，按 Codex `ModelInfo` 当前 schema 注册 Opus 4.7 / 4.6、Sonnet 4.6、Haiku 4.5。用户拷到本机 `~/.codex/` 并在 `~/.codex/config.toml` 里设：

```toml
model_catalog_json = "/home/<user>/.codex/model_catalog.json"   # 必须绝对路径
```

---

## 安装配置 / Installation

前提 / Prerequisite: 你已经有一份可跑的 LiteLLM 部署（容器名 `litellm`，工作目录含 `docker-compose.yml` / `config.yaml` / `.env`）。把本 repo 克隆到该工作目录（或至少把 `callbacks/` 拷过去）。

You already have a working LiteLLM deployment (container `litellm`, working dir containing `docker-compose.yml` / `config.yaml` / `.env`). Clone this repo into that working dir, or at least drop `callbacks/` in.

### LiteLLM 端 / LiteLLM side

**① 挂载 `callbacks/` 并让 Python 能 import / Mount `callbacks/` and make it importable**

编辑 `docker-compose.yml` 的 `litellm` service：

```yaml
services:
  litellm:
    environment:
      - PYTHONPATH=/app
      - PING_INTERVAL_SECONDS=${PING_INTERVAL_SECONDS:-30}
      - CHUNK_DELAY_SECONDS=${CHUNK_DELAY_SECONDS:-0}
    volumes:
      - ./callbacks:/app/callbacks:ro
```

**② 注册回调 / Register callbacks in `config.yaml`**

```yaml
litellm_settings:
  callbacks:
    - callbacks.codex_sanitizer.instance    # 先消毒 / sanitize first
    - callbacks.ping_injector.instance      # 再心跳 / heartbeat second
    # - callbacks.chunk_delayer.instance    # 仅测试 PingInjector 时启用
```

- **两者都要**挂才能同时服务 Codex 和 Claude Code
- 仅 Claude Code 使用：只挂 `ping_injector`
- 仅 Codex 使用：两个都要挂（`codex_sanitizer` 修字段问题，`ping_injector` 防长空闲）

注意写 `.instance`（预构建实例）而不是 `.CodexSanitizer` / `.PingInjector`（类） —— LiteLLM 用 `getattr` 解析点号路径，类形式会报错。

**③ `.env` 参数 / Environment variables**

```dotenv
PING_INTERVAL_SECONDS=30
# CHUNK_DELAY_SECONDS=300   # 仅测试时启用
```

**④ 重启 / Restart**

```bash
docker compose restart litellm
docker compose logs --tail=30 litellm    # 确认无 ImportError / Traceback
```

**⑤ 验证 / Verify**

验证 Codex 路径（模拟 Codex 的 payload）：

```bash
curl -N -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "model":"claude-opus-4-7",
       "input":[{"role":"user","content":"hi"}],
       "stream":true,
       "client_metadata":{"x-codex-installation-id":"test"},
       "include":["reasoning.encrypted_content"]
     }' \
     http://localhost:4000/v1/responses
```

期望：HTTP 200 + 完整 SSE 流，日志里无 `Extra inputs are not permitted`。

验证 Claude Code 路径的心跳：

```bash
curl -N -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
     -H "Content-Type: application/json" \
     -d '{"model":"claude-opus-4-7","stream":true,
          "messages":[{"role":"user","content":"慢慢数到 5"}]}' \
     http://localhost:4000/v1/messages
```

长空闲期应能看到 `event: ping`。

要强制触发 ping（造 hang 测试）：临时在 `.env` 设 `CHUNK_DELAY_SECONDS=300`、`config.yaml` 把 `chunk_delayer.instance` 加在 `ping_injector` **之前**，`docker compose restart litellm`。验证完记得改回。

---

### Codex 端 / Codex side（仅 Codex 用户）

**① 拷贝示例 catalog**

```bash
mkdir -p ~/.codex
cp docs/model_catalog.example.json ~/.codex/model_catalog.json
```

按需删减 —— catalog 里列出了 Opus 4.7 / 4.6、Sonnet 4.6、Haiku 4.5 四个模型，slug 必须和 LiteLLM `config.yaml` 里的 `model_name` 完全一致。

**② 配置 `~/.codex/config.toml`**

```toml
model = "claude-opus-4-7"
model_provider = "litellm"
model_catalog_json = "/home/<user>/.codex/model_catalog.json"   # 绝对路径

[model_providers.litellm]
name = "LiteLLM"
base_url = "http://<LiteLLM-host>:4000/v1"
env_key = "LITELLM_API_KEY"
wire_api = "responses"
request_max_retries = 2
stream_max_retries = 3
stream_idle_timeout_ms = 300000
```

**注意：**
- `model_catalog_json` 必须**绝对路径**（Codex 用 `AbsolutePathBuf`，相对路径报错）
- `wire_api` 只能是 `"responses"`（`"chat"` 在 Codex 上游已移除）

**③ 导出 key**

```bash
export LITELLM_API_KEY="<LiteLLM master key 或 virtual key>"
```

**④ 开新会话验证**

- ❌ `Model metadata for 'claude-opus-4-7' not found` warning 消失
- ❌ `BedrockException - client_metadata: Extra inputs are not permitted` 不再出现
- ✅ context window 显示为 1M（Opus 4.7）
- ✅ 长输出任务流稳定，不卡

更完整的 Codex 端说明见 [`docs/codex-setup.md`](docs/codex-setup.md)。

---

## 环境变量 / Environment variables

| Var | Default | 说明 / Meaning |
|-----|---------|----------------|
| `PING_INTERVAL_SECONDS` | `30` | 空闲多久后下发心跳；同时也是"帧密集时抑制心跳"的阈值 |
| `CHUNK_DELAY_SECONDS` | `0` | 仅测试用。`>0` 时 `ChunkDelayer` 强制延迟首帧；Codex 路径 pass-through 不受影响 |

---

## 流程图 / Flow

```
┌───────────────────────────────────────────────────────────────────────┐
│  litellm container  (ghcr.io/berriai/litellm)                         │
│                                                                       │
│   Claude Code / curl                Codex CLI                         │
│        │  POST /v1/messages              │  POST /v1/responses        │
│        ▼                                 ▼                            │
│   ┌──────────────────────────────────────────────────────────────┐    │
│   │            async_pre_call_hook                               │    │
│   │   callbacks.codex_sanitizer.instance                         │    │
│   │   ──────────────────────────────────                         │    │
│   │   if call_type == "aresponses":                              │    │
│   │       data.pop("client_metadata", None)                      │    │
│   │       filter data["include"]                                 │    │
│   │   (Anthropic / Chat paths untouched)                         │    │
│   └──────────────────────────────────────────────────────────────┘    │
│        │                                                              │
│        ▼                                                              │
│   LiteLLM Proxy core  ──►  Bedrock Converse  (Opus / Sonnet / Haiku)  │
│        ▲                                                              │
│        │   upstream SSE chunks  (可能空闲 30s+ / long idle gaps)      │
│        │                                                              │
│        ▼                                                              │
│   ┌──────────────────────────────────────────────────────────────┐    │
│   │   async_post_call_streaming_iterator_hook                    │    │
│   │   callbacks.ping_injector.instance                           │    │
│   │   ──────────────────────────────────                         │    │
│   │   frame = ':\n\n'    if is_responses_api(data)   (Codex)     │    │
│   │         = 'event: ping\ndata: {"type":"ping"}\n\n' (CC)      │    │
│   │                                                              │    │
│   │   ├── pump  task : async for chunk in response:              │    │
│   │   │                    q.put(("item", chunk))                │    │
│   │   ├── tick  task : while True:                               │    │
│   │   │                    await sleep(PING_INTERVAL_SECONDS)    │    │
│   │   │                    q.put(("ping", frame))                │    │
│   │   └── main  loop : kind, v = await q.get()                   │    │
│   │                    if kind == "ping" and                     │    │
│   │                       now - last_yield < interval: continue  │    │
│   │                    yield v                                   │    │
│   └──────────────────────────────────────────────────────────────┘    │
│        │                                                              │
│        ▼                                                              │
│   SSE serializer (LiteLLM) ──► text/event-stream ──► Client           │
│        Claude Code: sees `event: ping`, no fallback to non-stream     │
│        Codex:       sees `:\n\n`, idle watchdog reset, no disconnect  │
└───────────────────────────────────────────────────────────────────────┘
```

要点 / Key details:

- callback 注册写法是 `callbacks.<module>.instance`（**预构建的实例**） —— LiteLLM 用 `getattr` 解析点号路径，写成类会报错。
- 下发的是**预格式化 SSE 字符串**；dict 会被 LiteLLM 的 SSE 序列化器吞掉 `event:` 行和注释行。
- `tick` 只投候选，是否真发由 main loop 用 `last_yield` 判断 —— 这就是"帧密集不插心跳 / 空闲才补心跳"的语义。
- `codex_sanitizer` 的 `call_type` 判别是权威的；`ping_injector` 拿不到 `call_type`（LiteLLM v1.83.14 不塞进 `request_data`），只能 body 嗅探 `"input" in data and "messages" not in data`。共享在 `callbacks/_route.py`。
- `finally` 里显式 `await` 被取消的 pump/tick，避免遗留 "Task was destroyed but it is pending" 告警。

---

## 本 repo 追踪范围 / Tracked scope

本 repo **不是** LiteLLM 上游，也不含部署所需的 `config.yaml` / `docker-compose.yml` / `.env`（均在 `.gitignore`，由现有部署维护）。

| Path | Purpose |
|------|---------|
| `callbacks/codex_sanitizer.py` | Codex → Bedrock 字段消毒 / field sanitizer |
| `callbacks/ping_injector.py` | SSE 心跳 / SSE heartbeat |
| `callbacks/chunk_delayer.py` | 测试工具 / test helper (no-op by default) |
| `callbacks/_route.py` | 客户端判别 helper / client-type sniff helper |
| `docs/codex-setup.md` | Codex 完整配置指南 / full Codex setup guide |
| `docs/model_catalog.example.json` | Codex 模型 metadata 示例 / example model catalog |
| `docs/superpowers/specs/` | Design docs |
| `docs/superpowers/plans/` | Implementation plans |

`ChunkDelayer` 是一个**仅测试用**的 CustomLogger：`CHUNK_DELAY_SECONDS > 0` 时它会把 Anthropic 路径上游第一帧压住指定秒数，用来人为造一个慢 TTFB 的空窗口，验证 `PingInjector` 真的在空闲期发心跳。Codex 路径始终 pass-through 不受延迟。日常是 no-op。

---

## References

- [`docs/codex-setup.md`](docs/codex-setup.md) — Codex 端完整配置指南
- [`docs/model_catalog.example.json`](docs/model_catalog.example.json) — Codex model catalog 示例
- Design: [`docs/superpowers/specs/2026-05-09-ping-injector-design.md`](docs/superpowers/specs/2026-05-09-ping-injector-design.md)
- Plan: [`docs/superpowers/plans/2026-05-09-ping-injector.md`](docs/superpowers/plans/2026-05-09-ping-injector.md)
- LiteLLM `CustomLogger` hook: `async_pre_call_hook`, `async_post_call_streaming_iterator_hook`
- 上游 issue（Codex 硬编码 `client_metadata`）: https://github.com/openai/codex/issues/17910
