# Codex CLI × LiteLLM × Bedrock Claude — Setup Guide

把 OpenAI [Codex CLI](https://github.com/openai/codex) 接到 AWS Bedrock 上的 Anthropic Claude 模型（Opus / Sonnet / Haiku），中间过一层自部署的 LiteLLM。

---

## 架构

```
Codex CLI (本机)
   │  POST /v1/responses  (OpenAI Responses API, SSE stream)
   ▼
LiteLLM Proxy
   │  Bedrock Converse / converse-stream
   ▼
AWS Bedrock  (global.anthropic.claude-opus-4-7 等)
```

两端**协议方言不同**，需要中间层做几处显式翻译与补偿。

---

## 会遇到的三个问题

| 问题 | 表现 | 致命性 | 解决位置 |
|---|---|---|---|
| **1. Codex 请求带 Bedrock 不认的字段** | 每次对话返回 `BedrockException - client_metadata: Extra inputs are not permitted` | 对话直接 400 | LiteLLM 端 `CodexSanitizer` 回调 |
| **2. Codex 不识别自定义 slug** | 启动 warning `Model metadata for 'claude-opus-4-7' not found. Defaulting to fallback metadata` | 非致命但 context window 被压到 fallback 值 | Codex 端 `~/.codex/model_catalog.json` |
| **3. Bedrock 流长空闲导致客户端误判** | 长输出时 Codex idle timeout 断流 / Claude Code 回退非流式 | 表现为卡死 | LiteLLM 端 `PingInjector` 回调 |

### 问题 1 详情

Codex Rust 源码 `codex-rs/core/src/client.rs:761` 硬编码在 Responses API 请求体顶层注入：

```json
"client_metadata": {"x-codex-installation-id": "<uuid>"}
```

这不是 OpenAI Responses API 的规范字段（`openai/openai-openapi` 仓库 0 命中），真 OpenAI 静默忽略，Bedrock Converse 严格校验直接拒。Codex 还可能带 `include: ["reasoning.encrypted_content"]`，Bedrock 同样不认。上游 issue [`openai/codex#17910`](https://github.com/openai/codex/issues/17910) 已 WONTFIX。

**LiteLLM 原生 `drop_params` / `additional_drop_params` 救不了**：这俩只对 `/v1/chat/completions` 生效，`/v1/responses` 路径不跑（社区 issue #20515 / #25931 / #19225）。必须 proxy 侧写 `async_pre_call_hook` 剥字段。

### 问题 2 详情

Codex 内部维护模型 metadata 表（context window、reasoning 能力、truncation 策略等）。匹配规则是 `slug` 的 **longest-prefix match**。自定义 slug 命中不到时 `used_fallback_model_metadata = true`，触发 `codex-rs/core/src/session/turn_context.rs:770` 的 warning，并用 fallback 参数（context window 通常会被压到很小）。解法是让 Codex 读一份本地 `model_catalog.json`。

### 问题 3 详情

Bedrock Converse SSE 在推理阶段会出现 20-60s 级别的无帧空闲。两类客户端都对空闲敏感：

- **Codex**：`stream_idle_timeout_ms` 超时断流
- **Claude Code**：空闲 ≥ 阈值后判定 stream 异常，回退 non-stream 并把 `max_tokens` 裁到 21333，长输出必超时

Anthropic 官方 API 空闲期发 `event: ping` 规避。Bedrock 原生流没有，必须中间层补。

---

## 完整安装配置（LiteLLM 端）

以下步骤假设你已经有一套能跑的 LiteLLM 部署（容器名 `litellm`，工作目录含 `docker-compose.yml` / `config.yaml` / `.env`）。把本仓库 clone 到该工作目录，或至少把 `callbacks/` 拷过去。

### ① 挂载 `callbacks/` 并让 Python 能 import

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

`PYTHONPATH=/app` 让 `callbacks.codex_sanitizer` 和 `callbacks.ping_injector` 能 import；`:ro` 只读挂载。

### ② 注册 Claude 模型

`config.yaml`：

```yaml
model_list:
  - model_name: claude-opus-4-7
    litellm_params:
      model: bedrock/global.anthropic.claude-opus-4-7
      aws_region_name: us-east-1
      drop_params: true
    model_info:
      max_input_tokens: 1048576
      max_output_tokens: 65536
      supports_vision: true
      supports_prompt_caching: true

  - model_name: claude-sonnet-4-6
    litellm_params:
      model: bedrock/global.anthropic.claude-sonnet-4-6
      aws_region_name: us-east-1
      drop_params: true
```

### ③ 注册两个回调

`config.yaml`：

```yaml
litellm_settings:
  callbacks:
    - callbacks.codex_sanitizer.instance    # 先消毒，解决问题 1
    - callbacks.ping_injector.instance      # 再补心跳，解决问题 3
    # - callbacks.chunk_delayer.instance    # 仅在测试 PingInjector 时启用
```

- `codex_sanitizer` 只实现 `async_pre_call_hook`，只在 `call_type == "aresponses"` 时动作，对 Claude Code 流量（`anthropic_messages`）零影响。
- `ping_injector` 按 body 嗅探区分两类客户端：Anthropic 路径发 `event: ping`，Codex 路径发 SSE 注释帧 `:\n\n`（按 WHATWG SSE 规范被 Codex 的 `eventsource_stream` 在事件分发前消费掉，对解析器完全透明）。

### ④ `.env` 参数

```dotenv
PING_INTERVAL_SECONDS=30
# CHUNK_DELAY_SECONDS=300   # 仅测试时启用
```

### ⑤ 重启

```bash
docker compose restart litellm
docker compose logs --tail=30 litellm   # 确认无 ImportError / Traceback
```

### ⑥ LiteLLM 侧验证

模拟 Codex 的 payload（带两个 Bedrock 不认的字段）：

```bash
curl -N -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "model":"claude-opus-4-7",
       "input":[{"role":"user","content":"hi"}],
       "stream":true,
       "client_metadata":{"x-codex-installation-id":"test-uuid"},
       "include":["reasoning.encrypted_content"]
     }' \
     http://localhost:4000/v1/responses
```

期望：HTTP 200 + 完整 SSE 流，日志里无 `Extra inputs are not permitted`。

---

## 完整安装配置（Codex 端）

以下在**本机**（Codex CLI 所在机器）操作。

### ① 拷贝 `model_catalog.json`

从本仓库 `docs/model_catalog.example.json` 拷一份到 `~/.codex/model_catalog.json`。

JSON schema 对应 Codex Rust 源码 `codex-rs/protocol/src/openai_models.rs` 中的 `ModelInfo` 结构。关键字段：

| 字段 | 作用 |
|---|---|
| `slug` | 匹配键，与 LiteLLM `model_name` 保持一致；Codex 用 longest-prefix 匹配 |
| `context_window` / `max_context_window` | 实际上限（Opus 4.7 Bedrock 上是 1M） |
| `auto_compact_token_limit` | 到达此 token 数时自动压缩历史 |
| `supports_reasoning_summaries` | Claude 4.x 设 `true`，让 Codex 显示 thinking |
| `default_reasoning_level` | 默认 reasoning effort，可选 `minimal`/`low`/`medium`/`high`/`xhigh` |
| `truncation_policy` | 必填，现用 `{"mode":"bytes","limit":10000}` |

### ② 配置 `~/.codex/config.toml`

```toml
model = "claude-opus-4-7"
model_provider = "litellm"
model_catalog_json = "/home/<user>/.codex/model_catalog.json"   # 必须绝对路径

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
- `model_catalog_json` 必须是**绝对路径**，Codex 用的是 `AbsolutePathBuf`，相对路径会报错
- `wire_api` 必须是 `"responses"`（`"chat"` 在 Codex 上游已移除）
- `stream_idle_timeout_ms = 300000` 是 Codex 端兜底，和 `PingInjector` 一起形成双保险

### ③ 导出 API key

```bash
export LITELLM_API_KEY="<LiteLLM master key 或 virtual key>"
```

### ④ 验证

开 Codex 新会话：
- ❌ 原本的 `Model metadata for 'claude-opus-4-7' not found` warning 消失
- ❌ 原本的 `BedrockException - client_metadata: Extra inputs are not permitted` 不再出现
- ✅ 长输出任务流稳定，不卡
- ✅ context window 显示为 1M（Opus 4.7）

---

## 可能的其它 warning（与 LiteLLM 无关）

| Warning | 来源 | 处理 |
|---|---|---|
| `[features].codex_hooks is deprecated. Use [features].hooks instead.` | Codex 端配置改名 | `~/.codex/config.toml` 把 `[features].codex_hooks` 改成 `[features].hooks`，或启动加 `--enable hooks` |
| `N hooks need review before they can run` | Codex hooks 审批机制 | 在 Codex 里运行 `/hooks` 逐个确认 |
| `Skill descriptions were shortened to fit the 2% skills context budget` | Codex skills 系统 | 装 skill 过多时出现；不装额外 skill 可忽略 |

---

## 参考

- **Codex repo**: https://github.com/openai/codex
- **上游 issue**（Codex 硬编码 `client_metadata`）: https://github.com/openai/codex/issues/17910
- **LiteLLM 相关 issues**:
  - [#20515](https://github.com/BerriAI/litellm/issues/20515) Responses API `extra_body` 被忽略
  - [#25931](https://github.com/BerriAI/litellm/issues/25931) `additional_drop_params` 对 passthrough 无效
  - [#14365](https://github.com/BerriAI/litellm/issues/14365) / [#16141](https://github.com/BerriAI/litellm/issues/16141) Codex 同类字段问题（`prompt_cache_key`）
- **Codex `ModelInfo` schema 源码**: https://github.com/openai/codex/blob/main/codex-rs/protocol/src/openai_models.rs
- **Codex model catalog 加载**: https://github.com/openai/codex/blob/main/codex-rs/core/src/config/mod.rs
- **Codex Warning 触发点**: https://github.com/openai/codex/blob/main/codex-rs/core/src/session/turn_context.rs
