# LiteLLM 网关注入 SSE Ping 心跳事件 — 设计文档

- **状态**：待实现
- **日期**：2026-05-09
- **范围**：PRD Phase 1（方案 A，LiteLLM CustomLogger hook）
- **需求来源**：`https://www.feishu.cn/docx/IoZWdfUSpoh76fxzhgYceHDtnxh`

## 1. 目标

修复 Claude Code（CC）+ Bedrock 长输出场景下的 hang：CC 因 Bedrock SSE 空闲期没有心跳而误判 stream 异常，回退到 non-stream 并把 `max_tokens` 裁到 21333，最终卡死。

做法：在 LiteLLM 网关注入周期性 `{"type":"ping"}` SSE 事件，对齐 Anthropic 官方 API 的行为。

验收标准与 PRD §5 一致：
- 空闲 30s 时客户端能收到 `event: ping\ndata: {"type":"ping"}` 帧
- 帧密集期不发冗余 ping
- CC 长文件写入不再 hang、不再回退 non-stream
- Bedrock 原生错误码、正常事件流保持不变
- 网关 P99 延迟无可测量上升

## 2. 非目标

- 不实现 PRD Phase 2（前置 FastAPI 包装层）。Phase 1 实测失败时才启动。
- 不改 LiteLLM 源码、不引入新进程 / 新容器。
- 不新增日志、metrics、鉴权、审计（YAGNI；后续需要时单独做）。
- 不按模型 / 端点过滤，对所有流式请求无差别注入。

## 3. 总体架构

在现有 LiteLLM 部署（纯 docker-compose + config.yaml）上新增一个 `CustomLogger` 回调：

```
┌────────────────────────────────────────────────────────┐
│  litellm container (ghcr.io/berriai/litellm:v1.83.3…)  │
│                                                        │
│   /v1/messages (stream)                                │
│        │                                               │
│        ▼                                               │
│   LiteLLM Proxy core                                   │
│        │                                               │
│        ▼                                               │
│   async_post_call_streaming_iterator_hook              │
│   ──────────────────────────────────────────           │
│   callbacks.ping_injector.PingInjector ◄──── NEW       │
│        │                                               │
│        ├── pump:  async for chunk in response ──► q    │
│        ├── tick:  sleep(interval) → put("ping") ► q    │
│        └── loop:  q.get() → reset idle timer → yield   │
│        │                                               │
│        ▼                                               │
│   SSE serializer (LiteLLM)                             │
│        │                                               │
│        ▼                                               │
│   Client (Claude Code)                                 │
└────────────────────────────────────────────────────────┘
```

入侵点：
- 新文件：`callbacks/ping_injector.py`（repo 根）
- `docker-compose.yml` 加 volume 挂载 + `PYTHONPATH=/app` 环境变量 + `PING_INTERVAL_SECONDS=30`
- `config.yaml` 的 `litellm_settings.callbacks` 加 `callbacks.ping_injector.PingInjector`

## 4. 组件

### 4.1 `callbacks/ping_injector.py`

唯一对外接口：`PingInjector(CustomLogger)` 类，只实现 `async_post_call_streaming_iterator_hook(user_api_key_dict, response, request_data)`。

**方法契约**（async generator）：
- 入参 `response`：LiteLLM 转好的上游 async iterator
- 出参：`yield` 出去的对象被 LiteLLM 的 `async_sse_data_generator` 当成 SSE 帧序列化下发
- 除了在空闲期额外插入 `{"type": "ping"}` 之外，不变换任何上游 chunk

**内部结构**：

| 子任务 | 形式 | 职责 |
|-|-|-|
| `pump` | `asyncio.Task` | `async for chunk in response: q.put(("item", chunk))`；正常结束 `q.put(("done", None))`；异常 `q.put(("error", exc))` |
| `tick` | `asyncio.Task` | 循环 `await asyncio.sleep(interval)` + `q.put(("ping", {"type": "ping"}))` |
| 主循环 | generator body | `q.get()` 拿到 `item` / `ping` 就 yield；`done` → return；`error` → `raise` |

**空闲语义**：tick 只负责"每 interval 秒投一个 ping 候选进队列"；主循环维护 `last_yield = time.monotonic()`，拿到 ping 时看"距上次 yield 是否 ≥ interval"，不足则丢弃，达到才发。tick 永远不需要感知主循环状态。

**资源清理**：`try/finally` 里 `tick_t.cancel()` + `pump_t.cancel()`。

**参数化**：
- `PING_INTERVAL_SECONDS` 环境变量，默认 `30`
- `__init__` 时读一次，存为 `self.interval`

### 4.2 `docker-compose.yml`

两处改动（现有 `litellm` service 只有 `env_file:` 没有 `environment:` 段；本次新增 `environment:` 段，并在 `volumes:` 段追加一行）：

```yaml
  litellm:
    # ...existing keys (image / container_name / restart / depends_on / env_file / network_mode / command)...
    environment:                              # NEW section
      - PYTHONPATH=/app
      - PING_INTERVAL_SECONDS=${PING_INTERVAL_SECONDS:-30}
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./callbacks:/app/callbacks:ro         # NEW line
      - ./ai_usage_chat.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/ai_usage_chat.py
      - ./endpoints.py:/usr/lib/python3.13/site-packages/litellm/proxy/management_endpoints/usage_endpoints/endpoints.py
```

注意：
- compose 的 `environment:` 段会与 `env_file:.env` 合并，compose 段覆盖 env_file。当前 `.env`（见附录/repo 现状）没有 `PYTHONPATH` / `PING_INTERVAL_SECONDS`，不会冲突。
- `:ro` 只读挂载，callback 模块不需要容器写。

### 4.3 `config.yaml`

`litellm_settings` 下追加一行（现有 5 个 key 保留不变）：

```yaml
litellm_settings:
  request_timeout: 600
  set_verbose: false
  num_retries: 3
  modify_params: true
  drop_params: true
  callbacks: callbacks.ping_injector.PingInjector   # NEW
```

后续若需多个 callback，再改成列表形式。

## 5. 错误处理与边界情况

1. **上游异常（Bedrock 抛错、网络中断、超时）**：pump 捕获所有 `Exception` 入队；主循环 `raise` 保留原始 traceback 与类型。LiteLLM 原有错误处理链接管。
2. **客户端断开**：generator `.aclose()` → `finally` 里 cancel 两个 task；pump 的 `async for` propagate `CancelledError` 到上游，避免继续消费但丢帧。
3. **第一帧之前就发 ping**：`last_yield` 初始化为生成器启动的 monotonic 时刻；若上游 30s 内无任何输出，第 30s 发第一个 ping。正是目标场景。
4. **上游结束后残留 ping**：主循环看到 `done` 立即 return，不消费队列里剩余的 ping 候选；message_stop 后不会拖多余 ping。
5. **帧格式风险**：LiteLLM 的 `async_sse_data_generator` 可能只发 `data:` 不发 `event: ping`。代码层不做预防性兜底（不自己拼字符串 yield），实测不认时走 PRD Phase 2（方案 B）。该风险写进 §6 Test Plan 与 §8 Rollback。
6. **非流式请求**：hook 只在 stream 路径被调用，不用判断。

## 6. 测试与验收

### 6.1 curl 抓帧 — 确认 SSE 格式合规

```bash
curl -N -X POST http://localhost:4000/v1/messages \
  -H "anthropic-version: 2023-06-01" \
  -H "x-api-key: $LITELLM_MASTER_KEY" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-haiku-4-5",
    "stream": true,
    "max_tokens": 100,
    "messages": [{"role":"user","content":"等 90 秒后说 hi"}]
  }' | sed -n '1,80p'
```

**通过标准**：输出里出现 `event: ping\ndata: {"type":"ping"}`（带 `event:` 行）；或仅出现 `data: {"type":"ping"}` 但 §6.3 的 CC 端到端测试通过。

### 6.2 帧间隔验证

给一个快速持续吐 token 的 prompt（例如"数到 200"），观察：帧密集期 30s 内无冗余 ping。通过标准：ping 数量接近 `⌈idle_seconds / 30⌉`，不是 `total_seconds / 30`。

### 6.3 CC 长文件写入端到端

真实 CC 做一次"一次性写入长文件"（约 5~10 分钟单次输出），通过标准：
- CC 不 hang
- 不切到 non-stream（看 LiteLLM access log / CC 端行为）
- `max_tokens` 不再被裁到 21333

### 6.4 正常流回归

跑 1~2 次普通短对话，确认 `message_start` / `content_block_*` / `message_stop` 事件顺序与内容不被 ping 打乱。

### 6.5 错误传播验证

发一个故意的坏请求（如 `max_tokens: -1`），确认 Bedrock 4xx 原样透传到 CC，错误里不掺 ping。

## 7. 部署

```bash
docker compose up -d --force-recreate litellm
```

callbacks 目录是 volume mount，不需要重建镜像。

## 8. Rollback

删除 `config.yaml` 里 `callbacks: callbacks.ping_injector.PingInjector` 一行，执行 `docker compose restart litellm`。不需要回滚镜像或代码。

## 9. Upgrade Playbook

本实现依赖 LiteLLM 的三个点，其中两个非公共契约：

| 依赖点 | 性质 | 升级风险 |
|-|-|-|
| `async_post_call_streaming_iterator_hook` 的签名与"yield 额外 chunk"行为 | **非公共契约**。PRD 提到官方 feature request #14953（heartbeat）已被标为 not_planned | hook 行为变更需自行跟进 |
| `async_sse_data_generator` 的序列化输出（是否带 `event:` 行） | **内部函数**（`litellm/proxy/common_request_processing.py`）| 序列化规则变化 → ping 帧格式变化 |
| callback 注册路径（`PYTHONPATH` + 模块点路径） | **Python 标准机制** | 不会变 |

**升级时标准复测流程**（5 分钟）：

1. 改 `docker-compose.yml` 的 image 版本，`docker compose up -d`
2. 跑 §6.1 curl 抓帧 → 确认 ping 帧格式未变
3. 跑 §6.2 帧间隔验证 → 确认空闲语义未退化成固定周期
4. 跑一次 CC 普通短对话 → 正常流回归
5. 可选：跑 §6.3 CC 长文件场景

**失败模式**：

- **ping 帧格式变了**（LiteLLM 改了 `async_sse_data_generator`）：若 CC 不认，即触发 PRD Phase 2（方案 B）条件，上 FastAPI 包装层
- **hook 根本没被调用**（改名或签名变化）：容器日志会有 `TypeError` 或 `CustomLogger` 相关线索；临时回滚到旧 image tag，到 LiteLLM 仓库查 CHANGELOG / issue 再定

## 10. 参考

- Anthropic SSE 规范：`https://docs.claude.com/en/api/messages-streaming`
- LiteLLM `async_post_call_streaming_iterator_hook`：`https://docs.litellm.ai/docs/proxy/call_hooks`
- LiteLLM 源码入口：`litellm/proxy/common_request_processing.py::async_streaming_data_generator`
- LiteLLM heartbeat issue（not_planned）：`https://github.com/BerriAI/litellm/issues/14953`
- 需求来源：`https://www.feishu.cn/docx/IoZWdfUSpoh76fxzhgYceHDtnxh`
