# Kimiclaw A2A Gateway

A2A protocol (v1.0 subset) gateway fronting the local OpenClaw agent.

- Public URL: `https://kimiclaw.hoyack.ai/`
- Agent card: `GET /.well-known/agent.json` (public)
- JSON-RPC: `POST /` — bearer token required
- Origin: uvicorn on `127.0.0.1:8000`, tunnel via cloudflared systemd service

## Auth

`Authorization: Bearer $KIMICLAW_TOKEN` — token lives in `.env` (chmod 600, not committed).

## Methods

- `message/send` — blocking; returns completed task. Through Cloudflare, capped at ~120s per task. For longer turns use one of:
  - `?wait=false` query param (or `params.configuration.blocking=false`) — returns the task immediately in `submitted` state; track completion via `tasks/get` or the notify webhook.
  - `message/stream` — SSE with 15s keepalives.
- `tasks/get`, `tasks/cancel`

## Example

```bash
curl -s https://kimiclaw.hoyack.ai/ \
  -H "Authorization: Bearer $KIMICLAW_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc":"2.0","id":"1","method":"message/send",
    "params":{"message":{"role":"user","messageId":"m1",
      "parts":[{"kind":"text","text":"Summarize the systemd journal for yesterday"}]}}
  }'
```

## Session continuity

A2A `contextId` maps to an OpenClaw session (`a2a-<ctx>`). Reuse a contextId to continue a conversation.

## Ops

- `systemctl status kimiclaw-gateway cloudflared`
- Logs: `journalctl -u kimiclaw-gateway -f`
- Restart: `systemctl restart kimiclaw-gateway`

## Notify webhook

Disabled by default: set `GATEWAY_NOTIFY_URL` in `.env` to arm. On task completion/failure (configurable via `GATEWAY_NOTIFY_EVENTS`, default `task_completed,task_failed` — task_started intentionally excluded to prevent webhook fan-out loops) the gateway POSTs `{event, taskId, contextId, state?, reply?, durationMs?, timestamp}` with a raw `Authorization: <token>` header (no Bearer prefix). Fire-and-forget; failures are logged, never block the task.

Circuit breaker: after `GATEWAY_NOTIFY_FAIL_LIMIT` consecutive failures (default 5), outbound webhooks pause for `GATEWAY_NOTIFY_COOLDOWN_S` (default 900s).

Hard rule for the receiving flow: never call back into the gateway for `task_*` events. A single unfiltered round trip fans out exponentially (each task fired 2 webhooks → each webhook re-triggered the flow → each trigger created a new task). The n8n side MUST filter on `event` before this is re-armed.
