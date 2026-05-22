# Foundry Hosted Agent + Azure Web PubSub Prototype

This prototype keeps the Hosted Agent `invocations` programming model while moving token streaming to Azure Web PubSub.

The client flow is:

1. Client connects to Azure Web PubSub with a generated client access URL.
2. Client calls the Hosted Agent `POST /invocations` endpoint.
3. The agent returns an acknowledgement immediately after the turn is accepted.
4. The agent publishes chat events to Web PubSub for the client to receive over WebSocket.

## Project Layout

```text
shared/
  azure_openai_chat.py           # Foundry OpenAI streaming client
  chat_history.py                # Session filesystem chat history
  webpubsub_invocation_base.py   # Reusable Web PubSub invocation host/transport
agents/
  webpubsub/
    app.py                       # WebPubSubStreamingResponse-style entrypoint
    agent.yaml                   # Agent-specific Foundry manifest copy
  plain/
    app.py                       # Plain invocation SSE comparison entrypoint
  copilot/
    app.py                       # GitHub Copilot SDK entrypoint with bidirectional approvals
    agent.yaml                   # Agent-specific Foundry manifest copy
client/
  chat_client.py                 # Minimal Python client
  web_server.py                  # Browser chat UI server
  copilot_web_server.py          # Browser coding-agent UI with approval controls
```

## Configuration

Set these environment variables for Azure OpenAI:

```powershell
$env:AZURE_OPENAI_ENDPOINT="https://<your-account>.services.ai.azure.com/api/projects/<your-project>"
$env:AZURE_OPENAI_API_KEY="<your-api-key>"
$env:AZURE_OPENAI_DEPLOYMENT="<your-chat-deployment-name>"
```

`AZURE_OPENAI_API_KEY` is optional if the running identity has access to the Azure OpenAI resource. In that case the code uses `DefaultAzureCredential`.

The code uses `azure.ai.projects.aio.AIProjectClient` to create an authenticated Foundry OpenAI-compatible `/openai/v1/` client.

When deployed through `azd ai agent`, the code can also use the hosted-agent injected `FOUNDRY_PROJECT_ENDPOINT` and `AZURE_AI_MODEL_DEPLOYMENT_NAME` values.

Set these environment variables for Azure Web PubSub:

```powershell
$env:WEBPUBSUB_CONNECTION_STRING="<your Web PubSub connection string>"
$env:WEBPUBSUB_HUB="chat"
```

For local dry runs without Azure Web PubSub, set:

```powershell
$env:MOCK_WEBPUBSUB="true"
```

You can also put these values in a local `.env` file. See `.env.example` for the expected names.

For the GitHub Copilot agent, set:

```powershell
$env:WEBPUBSUB_TRANSPORT="client"
$env:COPILOT_GITHUB_TOKEN="<github-token-with-copilot-access>"
```

`WEBPUBSUB_TRANSPORT=client` is required for approval responses because the agent must receive Web PubSub group messages, not only publish messages. `COPILOT_MODEL` is optional; leave it unset to use the default model available to your Copilot account. If you set it to a model your plan or organization policy does not expose, session creation fails.

Set `COPILOT_WORKDIR` to the project folder Copilot should operate on. Avoid pointing it at a large parent directory because Copilot file tools have their own execution timeouts:

```powershell
$env:COPILOT_WORKDIR="C:\repo\hosted-agent-wps"
```

For `azd ai agent run`, make sure these values exist in the active azd environment:

```powershell
azd env set WEBPUBSUB_TRANSPORT "client"
azd env set WEBPUBSUB_AGENT_USER_ID "github-copilot-agent"
azd env set COPILOT_GITHUB_TOKEN "<github_pat_with_copilot_requests_read_write>"
azd env set COPILOT_WORKDIR "<local repo path for azd ai agent run, or /app when deployed>"
```

If Copilot reports that a built-in tool is unavailable, inspect the `session_tools_updated` event in the UI for exact tool names. You can force an allowlist with:

```powershell
azd env set COPILOT_AVAILABLE_TOOLS "<comma-separated-tool-names>"
```

## Run Locally

Install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start the Web PubSub hosted agent:

```powershell
python -m agents.webpubsub.app
```

Call it directly:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8088/invocations `
  -ContentType application/json `
  -Body '{"message":"hello","user_id":"local-user"}'
```

Run the client:

```powershell
python client/chat_client.py --agent-url http://localhost:8088/invocations --message "hello"
```

Run the browser UI:

```powershell
python client/web_server.py
```

Open:

```text
http://127.0.0.1:8090
```

The web server reads `WEBPUBSUB_CONNECTION_STRING` from `.env`, mints a scoped Web PubSub client URL for the browser, and proxies chat requests to `http://localhost:8088/invocations`. Override the agent URL with:

```powershell
python client/web_server.py --agent-url http://localhost:8088/invocations
```

For deployed Foundry agent endpoints, the web server uses `DefaultAzureCredential` and sends a bearer token with scope `https://ai.azure.com/.default`. Override with `FOUNDRY_AGENT_AUTH_SCOPE` if needed.

The web server also appends `api-version=2025-11-15-preview` to deployed agent URLs when it is missing. Override with `FOUNDRY_AGENT_API_VERSION` if your endpoint requires a different version.

The agent keeps chat history keyed by `conversation_id` under `$HOME/.webpubsub-chat/history`. The browser UI persists both `conversation_id` and `agent_session_id` in local storage. For deployed Hosted Agents, the web proxy forwards `agent_session_id` as a query parameter so Foundry can preserve the session filesystem across turns and idle resumes.

## Run The Copilot Agent

Start the Copilot hosted agent:

```powershell
$env:WEBPUBSUB_TRANSPORT="client"
$env:COPILOT_WORKDIR="C:\repo\hosted-agent-wps"
python -m agents.copilot.app
```

Run the Copilot-style browser UI:

```powershell
python client/copilot_web_server.py --agent-url http://localhost:8088/invocations
```

Open:

```text
http://127.0.0.1:8091
```

The Copilot agent accepts the same invocation request shape as the Web PubSub chat agent. It forwards Copilot SDK events such as streamed messages, tool events, status events, and approval requests to Web PubSub. When Copilot requests tool permission, the browser sends an `approval.resolved` event back to the per-stream Web PubSub group.

Web PubSub transport can run in two modes:

```powershell
azd env set WEBPUBSUB_TRANSPORT "service"
```

`service` uses the server-side Web PubSub service SDK and `send_to_user`. This is short-lived and simple, but output-only.

```powershell
azd env set WEBPUBSUB_TRANSPORT "client"
azd env set WEBPUBSUB_AGENT_USER_ID "hosted-agent"
```

`client` connects the agent to Web PubSub as a WebSocket client and publishes to a per-stream group. This is the foundation for bidirectional scenarios such as approvals and interrupts.

## Web PubSub Invocation Programming Model

Use `WebPubSubInvocationAgentServerHost` when you want the hosted agent to keep the normal `POST /invocations` entrypoint, return a fast `202` acknowledgement, and stream semantic events through Azure Web PubSub.

The invocation handler returns a `WebPubSubStreamingResponse`; `WebPubSubInvocationAgentServerHost` detects it, starts the Web PubSub publishing task, and returns the acknowledgement to the Hosted Agent platform:

```python
from shared.webpubsub_invocation_base import WebPubSubInvocationAgentServerHost, WebPubSubStreamingResponse

app = WebPubSubInvocationAgentServerHost()

@app.invoke_handler
async def invoke(request):
    body = await request.json()
    return WebPubSubStreamingResponse(
        stream_chat(body["message"]),
        user_id=body["user_id"],
        stream_id=body.get("stream_id"),
        conversation_id=body.get("conversation_id"),
    )
```

There are two supported ways to produce events.

### Yield `StreamEvent` Directly

This is the simplest model. Your async generator yields semantic events, and the host handles Web PubSub delivery and appends the final `done` event.

```python
from collections.abc import AsyncIterator

from shared.webpubsub_invocation_base import StreamEvent, WebPubSubStreamingResponse


async def stream_chat(message: str) -> AsyncIterator[StreamEvent]:
    yield StreamEvent(type="text.delta", data={"text": "Hello "})
    yield StreamEvent(type="text.delta", data={"text": message})
    yield StreamEvent(
        type="message.final",
        data={"role": "assistant", "content": f"Hello {message}"},
    )


@app.invoke_handler
async def invoke(request):
    body = await request.json()
    return WebPubSubStreamingResponse(
        stream_chat(body["message"]),
        user_id=body["user_id"],
        stream_id=body.get("stream_id"),
        conversation_id=body.get("conversation_id"),
    )
```

### Use `WebPubSubRunContext`

For richer agents, pass a factory function to `WebPubSubStreamingResponse`. The host calls it with a `WebPubSubRunContext` that contains invocation metadata and helper methods.

```python
from collections.abc import AsyncIterator

from shared.webpubsub_invocation_base import StreamEvent, WebPubSubRunContext, WebPubSubStreamingResponse


async def stream_chat(
    message: str,
    context: WebPubSubRunContext,
) -> AsyncIterator[StreamEvent]:
    yield context.text_delta("Thinking...\n")
    yield context.event("tool.event", name="search", query=message)
    yield context.final_message("Done.")


@app.invoke_handler
async def invoke(request):
    body = await request.json()
    return WebPubSubStreamingResponse(
        lambda context: stream_chat(body["message"], context),
        user_id=body["user_id"],
        stream_id=body.get("stream_id"),
        conversation_id=body.get("conversation_id"),
    )
```

The context exposes:

- `context.request_id`, `context.stream_id`, `context.user_id`, `context.conversation_id`
- `context.text_delta(text)` for token/text chunks
- `context.final_message(content, role="assistant")` for final assistant output
- `context.event(event_type, **data)` for custom semantic events
- `await context.publish_event(event)` to publish an event immediately instead of waiting for the generator queue
- `await context.wait_for_event(event_type, match=..., timeout=...)` to wait for inbound Web PubSub events

`wait_for_event` requires:

```powershell
WEBPUBSUB_TRANSPORT=client
```

The `service` transport can publish events but cannot receive browser responses. Use `client` transport for approvals, interrupts, and other bidirectional flows.

### Approval Example

```python
import uuid


async def stream_with_approval(context: WebPubSubRunContext) -> AsyncIterator[StreamEvent]:
    approval_id = str(uuid.uuid4())
    approval = context.event(
        "approval.requested",
        approval_id=approval_id,
        intention="Allow the agent to run a shell command",
    )

    await context.publish_event(approval)
    inbound = await context.wait_for_event(
        "approval.resolved",
        match={"approval_id": approval_id},
        timeout=600,
    )

    decision = (inbound.get("data") or {}).get("decision")
    if decision == "approved":
        yield context.event("tool.event", name="shell", status="approved")
    else:
        yield context.event("tool.event", name="shell", status="denied")
```

## Compare With Plain Invocation Streaming

This repo also includes a second agent that streams directly over the invocation HTTP response with SSE and does not use Azure Web PubSub:

```powershell
python -m agents.plain.app
```

Call it:

```powershell
Invoke-WebRequest `
  -Method Post `
  -Uri http://localhost:8088/invocations `
  -ContentType application/json `
  -Body '{"message":"hello"}'
```

The programming model difference is:

```text
WebPubSub agent:
  Developer yields semantic StreamEvent objects.
  WebPubSubInvocationAgentServerHost returns a 202 acknowledgement and publishes events to Web PubSub.
  Client receives events over WebSocket.

Plain invocation agent:
  Developer returns a StreamingResponse from the invocation handler.
  The invocation HTTP response remains open while tokens stream.
  Client receives events from the original HTTP request.
```

## Invocation Request Shape

```json
{
  "message": "hello",
  "user_id": "user-123",
  "conversation_id": "conversation-abc",
  "stream_id": "optional-client-stream-id"
}
```

The agent publishes Web PubSub server messages to `user_id`. Each event contains:

```json
{
  "type": "text.delta",
  "request_id": "...",
  "stream_id": "...",
  "sequence": 1,
  "data": {
    "text": "..."
  }
}
```

Completion is signaled with `type: "done"`. Failures are signaled with `type: "error"`.

## Hosted Agent Notes

This prototype uses `azure-ai-agentserver-invocations`, so the deployed container should expose the normal Hosted Agent invocations protocol. The Web PubSub connection string is a prototype convenience; for production, prefer Managed Identity or a secret store and ensure clients receive only scoped Web PubSub client access URLs.

## Deploy To Foundry

The active `azd up` configuration is the root `azure.yaml`, root `Dockerfile`, and root `agent.yaml`. The root `agent.yaml` is required by the Foundry `azd` agent extension and currently describes the `github-copilot-agent` deployment target.

The per-agent `agent.yaml` files under `agents/` are kept as reference manifests from earlier experiments. They are not used by the current `azd up` path.

If you need to initialize or recreate the current `azd` agent service from a manifest, use the root manifest with the current directory as the source root:

```powershell
azd ai agent init -m .\agent.yaml --src .
```

The container image contains `shared/` and `agents/`; the root `Dockerfile` starts the Copilot agent with `python -m agents.copilot.app`.

This repo currently registers one `azd` agent service:

```powershell
azd ai agent run github-copilot-agent
```

Use the service name when showing or invoking the deployed agent:

```powershell
azd ai agent show github-copilot-agent
```

If you already have a Foundry project:

```powershell
azd ai agent init `
  -m .\agent.yaml `
  --src . `
  --project-id "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.CognitiveServices/accounts/<account>/projects/<project>"
```

Set required runtime values:

```powershell
azd env set WEBPUBSUB_CONNECTION_STRING "<your Web PubSub connection string>"
azd env set WEBPUBSUB_HUB "chat"
azd env set CHAT_HISTORY_MAX_MESSAGES "20"
azd env set COPILOT_GITHUB_TOKEN "<github-token-with-copilot-access>"
azd env set COPILOT_AGENT_DEBUG_LOGS "true"
```

Deploy:

```powershell
azd up
```

Monitor deployed session logs:

```powershell
azd ai agent monitor github-copilot-agent --session-id <foundry-session-id> --follow
```
