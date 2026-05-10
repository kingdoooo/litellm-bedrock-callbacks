# litellm-bedrock-callbacks

> 自部署 LiteLLM 网关的 **CustomLogger 扩展** / CustomLogger add-ons for a self-hosted LiteLLM gateway.

---

## 问题背景 / Problem background

> 这是一个 **Claude Code 客户端 ↔ Bedrock 原生 SSE** 之间的兼容性问题，与 LiteLLM 本身无关。本 repo 只是因为我们的部署**恰好**使用 LiteLLM 作为网关，才顺势在它的扩展点上打补丁；换成任何在 CC 和 Bedrock 中间的其他代理层，方案是一样的。
>
> This is a compatibility issue between **Claude Code's client and Bedrock's native SSE stream** — it has nothing to do with LiteLLM itself. This repo patches the symptom at the LiteLLM layer simply because that's the gateway we happen to run; any proxy sitting between CC and Bedrock would do.

**中文**

Claude Code（CC）调用 Bedrock 上的 Anthropic 模型（Opus / Sonnet）时，长输出（长文件写入、长推理）会**卡住**：

- Bedrock Converse 的 SSE 流在推理阶段会出现**长时间无帧**的空闲窗口（几十秒级）
- CC 的客户端在空闲 ≥ 某阈值后判定 stream 异常，**回退成 non-stream**，同时把 `max_tokens` 裁到 21333
- 回退后的请求在长输出下直接超时 / 内容截断，用户体感就是"卡死"

Anthropic 官方 API 的解决办法是**在空闲期下发 `event: ping` 心跳帧**——只要客户端在空窗期仍收到字节，就不会误判。Bedrock 原生流**没有这个心跳**，所以只要 CC 和 Bedrock 中间没有一层主动补 ping 的代理，就会出现上述 hang。

**English**

When Claude Code calls Anthropic models on Bedrock (Opus / Sonnet), long-output requests **hang**:

- Bedrock Converse's SSE stream has long idle windows (tens of seconds) during reasoning
- After enough idle time, Claude Code assumes the stream is broken, **falls back to non-streaming**, and clips `max_tokens` to 21333
- For long outputs the fallback request times out or truncates — the user just sees a hang

Anthropic's own API avoids this by emitting `event: ping` heartbeat frames during idle periods — as long as the client keeps seeing bytes, it won't bail. Bedrock's native stream has no such heartbeat, so unless something in front of Bedrock synthesizes pings, any CC → Bedrock path will eventually hang.

---

## 本 repo 的设计 / How this repo fixes it

**中文**

既然问题本质是"空闲期没字节"，任何中间代理层插一个周期性心跳帧都能解决。我们的部署里代理层是 LiteLLM，所以我们挑 LiteLLM 自带的 `CustomLogger` 扩展点 `async_post_call_streaming_iterator_hook` 挂一个包装器 `PingInjector`；它位于 "LiteLLM 拿到上游 SSE chunks" 和 "SSE 序列化器把帧写回客户端" 之间——这是改 SSE 行为的最后一个干净的插入点。

实现上用一个 `asyncio.Queue` 把三件事解耦：

- **pump** 任务：消费上游 async 迭代器，把真实 chunk 塞进 queue
- **tick** 任务：每 `PING_INTERVAL_SECONDS` 秒往 queue 塞一个"ping 候选"
- **main loop**：从 queue 里拿东西往下 yield；但 ping 候选要通过"距上次 yield ≥ interval"这个 idle-aware 检查，否则丢弃——保证帧密集期不会插多余的 ping

不改 LiteLLM 源码、不引入新容器、不加新日志指标，对所有模型的流式响应都生效，不影响 Bedrock 原生错误码和正常事件流。

**English**

The underlying fix is simply "inject a heartbeat frame during idle windows" — any intermediary proxy could do it. In our deployment the proxy happens to be LiteLLM, so we reuse its built-in `CustomLogger` extension point `async_post_call_streaming_iterator_hook` and hook a wrapper called `PingInjector` there. It sits between "LiteLLM receives upstream SSE chunks" and "LiteLLM's SSE serializer writes to the client" — the last clean place to shape the SSE stream.

Three coroutines coordinate through one `asyncio.Queue`:

- **pump** — consumes the upstream async iterator and puts real chunks into the queue
- **tick** — every `PING_INTERVAL_SECONDS` seconds, puts a *ping candidate* into the queue
- **main loop** — pulls items and yields them downstream; ping candidates must pass an idle-aware check (`now - last_yield ≥ interval`), otherwise they're dropped — so dense-chunk windows don't get redundant pings

No LiteLLM source fork, no extra container, no new logging/metrics. Works for every streaming model, preserves Bedrock's native error codes and event stream.

---

## 流程图 / Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  litellm container  (ghcr.io/berriai/litellm:v1.83.3-*)          │
│                                                                  │
│   Client (Claude Code / curl)                                    │
│        │  POST /v1/messages  (stream:true)                       │
│        ▼                                                         │
│   LiteLLM Proxy core  ──►  Bedrock Converse  (Opus / Sonnet)     │
│        ▲                                                         │
│        │   upstream SSE chunks  (可能空闲 30s+ / long idle gaps) │
│        │                                                         │
│        ▼                                                         │
│   async_post_call_streaming_iterator_hook                        │
│   ──────────────────────────────────────────                     │
│   callbacks.ping_injector.instance   ◄──── NEW (this repo)       │
│        │                                                         │
│        ├── pump  task :  async for chunk in response:            │
│        │                     q.put(("item", chunk))              │
│        │                                                         │
│        ├── tick  task :  while True:                             │
│        │                     await sleep(PING_INTERVAL_SECONDS)  │
│        │                     q.put(("ping", _PING_FRAME))        │
│        │                                                         │
│        └── main  loop :  kind, v = await q.get()                 │
│                          if kind == "done":  return              │
│                          if kind == "error": raise v             │
│                          if kind == "ping"                       │
│                             and now - last_yield < interval:     │
│                                 continue         # drop ping     │
│                          yield v                                 │
│                          last_yield = now                        │
│                                                                  │
│   _PING_FRAME = 'event: ping\ndata: {"type":"ping"}\n\n'         │
│        │                                                         │
│        ▼                                                         │
│   SSE serializer (LiteLLM)                                       │
│        │   text/event-stream                                     │
│        ▼                                                         │
│   Client  — sees heartbeat, no longer falls back to non-stream   │
└──────────────────────────────────────────────────────────────────┘
```

要点 / Key details:

- callback 注册写法是 `callbacks.ping_injector.instance`（**预构建的实例**，不是类）——LiteLLM 用 `getattr` 解析点号路径，写成 `.PingInjector`（类）会报错。
  Register as `callbacks.ping_injector.instance` — LiteLLM resolves dotted paths via `getattr`, so the class form won't work.
- 下发的是**预格式化 SSE 字符串** `event: ping\ndata: {"type":"ping"}\n\n`；dict 会被 LiteLLM 的 SSE 序列化器丢掉 `event:` 行。
  The ping payload is a **pre-formatted SSE string**; a dict would lose the `event:` line during serialization.
- `tick` 只投候选，是否真发由 main loop 用 `last_yield` 判断——这就是"帧密集不塞 ping / 空闲才补 ping"的语义。
  `tick` only *proposes* pings; `main loop` decides — this is what gives the "dense: suppress / idle: inject" semantics.
- `finally` 里显式 `await` 被取消的 pump/tick，避免遗留 "Task was destroyed but it is pending" 告警。
  The `finally` block awaits cancelled tasks to avoid "Task was destroyed but it is pending" warnings.

---

## 本 repo 追踪范围 / Tracked scope

本 repo **不是** LiteLLM 上游，也不含部署所需的 `config.yaml` / `docker-compose.yml` / `.env`（均在 `.gitignore`，由现有部署维护）。
This repo is **not** LiteLLM upstream. It does not track `config.yaml` / `docker-compose.yml` / `.env` — those belong to the deployment.

| Path | Purpose |
|------|---------|
| `callbacks/ping_injector.py` | `PingInjector` 主功能 / main feature |
| `callbacks/chunk_delayer.py` | 测试工具 / test helper (no-op by default) |
| `docs/superpowers/specs/` | Design docs |
| `docs/superpowers/plans/` | Implementation plans |

`ChunkDelayer` 是一个**仅测试用**的 CustomLogger：`CHUNK_DELAY_SECONDS > 0` 时它会把上游第一帧压住指定秒数，用来人为造一个慢 TTFB 的空窗口，验证 `PingInjector` 真的在空闲期发 ping。日常是 no-op。
`ChunkDelayer` is a **test-only** CustomLogger: when `CHUNK_DELAY_SECONDS > 0`, it delays the first upstream chunk by that many seconds so you can synthesize a slow-TTFB idle window and prove `PingInjector` fires. No-op by default.

---

## 从零配置（5 步） / Configuration from scratch (5 steps)

前提 / Prerequisite: 你已经有一份可跑的 LiteLLM 部署（容器名 `litellm`，工作目录含 `docker-compose.yml` / `config.yaml` / `.env`）。把本 repo 克隆到同一工作目录（或至少把 `callbacks/` 拷过去）。
You already have a working LiteLLM deployment (container `litellm`, working dir containing `docker-compose.yml` / `config.yaml` / `.env`). Clone this repo into that working dir, or at least drop `callbacks/` in.

**① 挂载 `callbacks/` 并让 Python 能 import / Mount `callbacks/` and make it importable**

编辑 `docker-compose.yml` 的 `litellm` service / Extend the `litellm` service:

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

`PYTHONPATH=/app` 让 `callbacks.ping_injector` 能 import；`:ro` 只读挂载避免容器误写。
`PYTHONPATH=/app` makes `callbacks.ping_injector` importable; `:ro` is hygiene.

**② 在 `config.yaml` 注册 callback / Register the callback in `config.yaml`**

```yaml
litellm_settings:
  callbacks:
    - callbacks.ping_injector.instance
    # 仅在需要造 hang 测试时加上 / only when you want to synthesize a hang for testing:
    # - callbacks.chunk_delayer.instance
```

注意写 `.instance`（实例）而非 `.PingInjector`（类）。
Use `.instance` (the pre-built instance), not `.PingInjector` (the class).

**③ 在 `.env` 设置参数 / Set parameters in `.env`**

```dotenv
PING_INTERVAL_SECONDS=30
# CHUNK_DELAY_SECONDS=300   # 仅测试时启用 / only when testing
```

**④ 重启 / Restart**

```bash
docker compose restart litellm
docker compose logs --tail=30 litellm    # 确认无 ImportError / Traceback
```

**⑤ 验证 ping 帧确实下发 / Verify ping frames actually reach the client**

用 `curl -N`（不缓冲）看 SSE 流——长空闲时应能看到 `event: ping` 帧。
Use `curl -N` (unbuffered) to inspect the SSE stream — during idle gaps you should see `event: ping` frames.

```bash
curl -N -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
     -H "Content-Type: application/json" \
     -d '{"model":"claude-opus-4-7","stream":true,
          "messages":[{"role":"user","content":"慢慢数到 5 / count to 5 slowly"}]}' \
     http://localhost:4000/v1/messages
```

期望在输出里看到 / Expected in output:

```
event: ping
data: {"type":"ping"}
```

想强制触发 ping（造 hang 测试）：临时把 `.env` 的 `CHUNK_DELAY_SECONDS` 设 `300`、并在 `config.yaml` callbacks 列表中把 `callbacks.chunk_delayer.instance` 加在 `ping_injector` **之前**，`docker compose restart litellm`。验证完记得改回。
To force pings deterministically: temporarily set `CHUNK_DELAY_SECONDS=300` in `.env`, add `callbacks.chunk_delayer.instance` **before** `ping_injector` in `config.yaml`, restart. Revert afterwards.

---

## 环境变量 / Environment variables

| Var | Default | 说明 / Meaning |
|-----|---------|----------------|
| `PING_INTERVAL_SECONDS` | `30` | 空闲多久后下发 ping；同时也是"帧密集时抑制 ping"的阈值 / Idle threshold before a ping is emitted; also the dedup window during dense chunks |
| `CHUNK_DELAY_SECONDS` | `0` | 仅测试用。`>0` 时 `ChunkDelayer` 强制延迟首帧这么多秒 / Test-only. When > 0, `ChunkDelayer` holds the first upstream chunk for this many seconds |

---

## References

- Design: [`docs/superpowers/specs/2026-05-09-ping-injector-design.md`](docs/superpowers/specs/2026-05-09-ping-injector-design.md)
- Plan: [`docs/superpowers/plans/2026-05-09-ping-injector.md`](docs/superpowers/plans/2026-05-09-ping-injector.md)
- LiteLLM `CustomLogger` hook: `async_post_call_streaming_iterator_hook`
