from __future__ import annotations

import argparse
import asyncio
import os
import uuid
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from azure.identity import DefaultAzureCredential
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from dotenv import load_dotenv
from hypercorn.asyncio import serve
from hypercorn.config import Config
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Hosted Agent Web PubSub Chat</title>
  <style>
    :root {
      --ink: #18201c;
      --muted: #5d6a63;
      --line: #d5ddd6;
      --paper: #f7f4ec;
      --panel: #ffffff;
      --accent: #006c67;
      --accent-2: #d94f30;
      --soft: #e8f2ee;
      --shadow: 0 18px 60px rgba(24, 32, 28, 0.14);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: "Segoe UI", "Aptos", sans-serif;
      background:
        linear-gradient(135deg, rgba(0, 108, 103, 0.11), transparent 32%),
        linear-gradient(315deg, rgba(217, 79, 48, 0.1), transparent 28%),
        var(--paper);
    }

    main {
      display: grid;
      grid-template-rows: auto 1fr auto;
      width: min(1040px, calc(100vw - 32px));
      height: calc(100vh - 32px);
      margin: 16px auto;
      overflow: hidden;
      background: rgba(255, 255, 255, 0.82);
      border: 1px solid rgba(24, 32, 28, 0.12);
      border-radius: 8px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 20px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.62);
    }

    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: 0;
    }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-width: 132px;
      justify-content: flex-end;
      color: var(--muted);
      font-size: 13px;
    }

    .dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: #9a9f9b;
    }

    .dot.connected {
      background: var(--accent);
      box-shadow: 0 0 0 4px rgba(0, 108, 103, 0.14);
    }

    .messages {
      overflow-y: auto;
      padding: 22px 20px;
    }

    .message {
      display: grid;
      gap: 6px;
      max-width: 780px;
      margin-bottom: 18px;
    }

    .message.user {
      margin-left: auto;
      justify-items: end;
    }

    .role {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .bubble {
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      padding: 13px 15px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      line-height: 1.45;
      font-size: 15px;
    }

    .user .bubble {
      color: #fff;
      background: var(--accent);
      border-color: var(--accent);
    }

    .assistant.pending .bubble::after {
      content: "";
      display: inline-block;
      width: 7px;
      height: 16px;
      margin-left: 3px;
      vertical-align: -3px;
      background: var(--accent);
      animation: blink 1s steps(2, jump-none) infinite;
    }

    @keyframes blink {
      50% { opacity: 0; }
    }

    form {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 16px;
      border-top: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.76);
    }

    textarea {
      min-height: 54px;
      max-height: 150px;
      resize: vertical;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 13px 14px;
      color: var(--ink);
      background: #fff;
      font: inherit;
      outline: none;
    }

    textarea:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(0, 108, 103, 0.13);
    }

    button {
      align-self: end;
      height: 54px;
      min-width: 104px;
      border: 0;
      border-radius: 8px;
      color: #fff;
      background: var(--accent);
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }

    button:disabled {
      cursor: not-allowed;
      opacity: 0.55;
    }

    .error {
      color: var(--accent-2);
    }

    @media (max-width: 640px) {
      main {
        width: 100vw;
        height: 100vh;
        margin: 0;
        border: 0;
        border-radius: 0;
      }

      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .status {
        justify-content: flex-start;
      }

      form {
        grid-template-columns: 1fr;
      }

      button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Hosted Agent Web PubSub Chat</h1>
      <div class="status"><span id="dot" class="dot"></span><span id="status">Connecting</span></div>
    </header>
    <section id="messages" class="messages" aria-live="polite"></section>
    <form id="form">
      <textarea id="input" placeholder="Ask the hosted agent..." autocomplete="off"></textarea>
      <button id="send" type="submit" disabled>Send</button>
    </form>
  </main>

  <script>
    const messages = document.getElementById("messages");
    const form = document.getElementById("form");
    const input = document.getElementById("input");
    const send = document.getElementById("send");
    const statusText = document.getElementById("status");
    const dot = document.getElementById("dot");

    const userId = localStorage.getItem("wps:userId") || `web-${crypto.randomUUID()}`;
    const conversationId = localStorage.getItem("wps:conversationId") || `conversation-${crypto.randomUUID()}`;
    const agentSessionId = localStorage.getItem("wps:agentSessionId") || `session-${crypto.randomUUID()}`;
    localStorage.setItem("wps:userId", userId);
    localStorage.setItem("wps:conversationId", conversationId);
    localStorage.setItem("wps:agentSessionId", agentSessionId);

    const pending = new Map();
    let socket;

    function setStatus(text, connected = false) {
      statusText.textContent = text;
      dot.classList.toggle("connected", connected);
      send.disabled = !connected;
    }

    function addMessage(role, text = "", pendingState = false) {
      const item = document.createElement("article");
      item.className = `message ${role}${pendingState ? " pending" : ""}`;

      const label = document.createElement("div");
      label.className = "role";
      label.textContent = role === "user" ? "You" : "Agent";

      const bubble = document.createElement("div");
      bubble.className = "bubble";
      bubble.textContent = text;

      item.append(label, bubble);
      messages.append(item);
      messages.scrollTop = messages.scrollHeight;
      return { item, bubble };
    }

    function parseWebPubSubMessage(raw) {
      const message = JSON.parse(raw);
      if (message.type !== "message" || message.from !== "server") {
        if (message.type === "message" && message.from === "group") {
          return typeof message.data === "string" ? JSON.parse(message.data) : message.data;
        }
        return null;
      }

      return typeof message.data === "string" ? JSON.parse(message.data) : message.data;
    }

    async function connect() {
      setStatus("Connecting");
      const response = await fetch(`/negotiate?user_id=${encodeURIComponent(userId)}`);
      if (!response.ok) {
        throw new Error(await response.text());
      }

      const { url } = await response.json();
      socket = new WebSocket(url, "json.webpubsub.azure.v1");

      socket.addEventListener("open", () => setStatus("Connected", true));
      socket.addEventListener("close", () => {
        setStatus("Disconnected");
        setTimeout(() => connect().catch(showError), 1500);
      });
      socket.addEventListener("error", () => setStatus("Connection error"));
      socket.addEventListener("message", event => {
        const data = parseWebPubSubMessage(event.data);
        if (!data || !pending.has(data.stream_id)) {
          return;
        }

        const target = pending.get(data.stream_id);
        if (data.type === "text.delta") {
          target.bubble.textContent += data.data?.text || "";
        } else if (data.type === "message.final") {
          target.bubble.textContent = data.data?.content || target.bubble.textContent;
        } else if (data.type === "error") {
          target.bubble.classList.add("error");
          target.bubble.textContent = data.data?.message || "Stream failed.";
        } else if (data.type === "done") {
          target.item.classList.remove("pending");
          pending.delete(data.stream_id);
        }
        messages.scrollTop = messages.scrollHeight;
      });
    }

    function showError(error) {
      setStatus("Error");
      const target = addMessage("assistant", error.message || String(error));
      target.bubble.classList.add("error");
    }

    form.addEventListener("submit", async event => {
      event.preventDefault();
      const message = input.value.trim();
      if (!message || socket?.readyState !== WebSocket.OPEN) {
        return;
      }

      input.value = "";
      addMessage("user", message);

      const streamId = `stream-${crypto.randomUUID()}`;
      const assistant = addMessage("assistant", "", true);
      pending.set(streamId, assistant);

      try {
        joinStreamGroup(streamId);
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message,
            user_id: userId,
            conversation_id: conversationId,
            agent_session_id: agentSessionId,
            stream_id: streamId
          })
        });

        if (!response.ok) {
          throw new Error(await response.text());
        }
      } catch (error) {
        assistant.item.classList.remove("pending");
        assistant.bubble.classList.add("error");
        assistant.bubble.textContent = error.message || String(error);
        pending.delete(streamId);
      }
    });

    input.addEventListener("keydown", event => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        form.requestSubmit();
      }
    });

    function joinStreamGroup(streamId) {
      if (socket?.readyState !== WebSocket.OPEN) {
        return;
      }

      socket.send(JSON.stringify({
        type: "joinGroup",
        group: `stream-${streamId}`,
        ackId: Date.now()
      }));
    }

    connect().catch(showError);
  </script>
</body>
</html>
"""


def create_app(agent_url: str) -> Starlette:
    load_dotenv()
    agent_url = _with_agent_api_version(agent_url)

    connection_string = os.getenv("WEBPUBSUB_CONNECTION_STRING")
    hub = os.getenv("WEBPUBSUB_HUB", "chat")
    if not connection_string:
        raise RuntimeError("WEBPUBSUB_CONNECTION_STRING is required for the web UI.")

    service = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub)
    credential = DefaultAzureCredential()
    auth_scope = os.getenv("FOUNDRY_AGENT_AUTH_SCOPE", "https://ai.azure.com/.default")
    should_auth_agent = _should_auth_agent_url(agent_url)

    async def index(_: Request) -> HTMLResponse:
        return HTMLResponse(HTML)

    async def negotiate(request: Request) -> JSONResponse:
        user_id = request.query_params.get("user_id") or f"web-{uuid.uuid4()}"
        token = await asyncio.to_thread(
            service.get_client_access_token,
            user_id=user_id,
            roles=["webpubsub.joinLeaveGroup", "webpubsub.sendToGroup"],
        )
        return JSONResponse({"url": token["url"], "user_id": user_id})

    async def chat(request: Request) -> JSONResponse:
        body: dict[str, Any] = await request.json()
        target_url = _with_agent_session_id(agent_url, body.get("agent_session_id"))
        headers = await asyncio.to_thread(
            _build_agent_headers,
            credential,
            auth_scope,
            should_auth_agent,
        )
        response = await asyncio.to_thread(
            requests.post,
            target_url,
            json=body,
            headers=headers,
            timeout=30,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"content": response.text}

        return JSONResponse(payload, status_code=response.status_code)

    return Starlette(
        routes=[
            Route("/", index, methods=["GET"]),
            Route("/negotiate", negotiate, methods=["GET"]),
            Route("/api/chat", chat, methods=["POST"]),
        ]
    )


def _should_auth_agent_url(agent_url: str) -> bool:
    parsed = urlparse(agent_url)
    return parsed.hostname not in {"localhost", "127.0.0.1", "::1"}


def _with_agent_api_version(agent_url: str) -> str:
    parsed = urlparse(agent_url)
    if parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
        return agent_url

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "api-version" in query:
        return agent_url

    query["api-version"] = os.getenv("FOUNDRY_AGENT_API_VERSION", "2025-11-15-preview")
    return urlunparse(parsed._replace(query=urlencode(query)))


def _with_agent_session_id(agent_url: str, session_id: str | None) -> str:
    if not session_id:
        return agent_url

    parsed = urlparse(agent_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["agent_session_id"] = session_id
    return urlunparse(parsed._replace(query=urlencode(query)))


def _build_agent_headers(
    credential: DefaultAzureCredential,
    auth_scope: str,
    should_auth_agent: bool,
) -> dict[str, str]:
    if not should_auth_agent:
        return {}

    token = credential.get_token(auth_scope).token
    return {"Authorization": f"Bearer {token}"}


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument(
        "--agent-url",
        default=os.getenv("AGENT_INVOCATION_URL", "http://localhost:8088/invocations"),
    )
    args = parser.parse_args()

    config = Config()
    config.bind = [f"{args.host}:{args.port}"]

    print(f"Web UI: http://{args.host}:{args.port}")
    print(f"Agent invocation URL: {args.agent_url}")
    asyncio.run(serve(create_app(args.agent_url), config))


if __name__ == "__main__":
    main()
