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
  <title>Hosted Copilot Agent</title>
  <style>
    :root {
      --bg: #0f1419;
      --panel: #111b22;
      --terminal: #071014;
      --ink: #d4e7de;
      --muted: #7e938b;
      --line: #27413a;
      --green: #47d18c;
      --amber: #f2b84b;
      --red: #ff6b5f;
      --blue: #5fb3ff;
    }

    * { box-sizing: border-box; }

    html,
    body {
      width: 100%;
      height: 100%;
      overflow: hidden;
    }

    body {
      margin: 0;
      color: var(--ink);
      font-family: "Cascadia Code", "JetBrains Mono", "SFMono-Regular", monospace;
      background:
        radial-gradient(circle at top left, rgba(71, 209, 140, 0.16), transparent 34rem),
        radial-gradient(circle at bottom right, rgba(95, 179, 255, 0.12), transparent 30rem),
        var(--bg);
    }

    main {
      display: grid;
      grid-template-rows: auto 1fr auto;
      width: min(1180px, calc(100vw - 28px));
      height: calc(100dvh - 28px);
      margin: 14px auto;
      min-height: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      background: rgba(17, 27, 34, 0.86);
      box-shadow: 0 26px 90px rgba(0, 0, 0, 0.42);
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(7, 16, 20, 0.7);
    }

    h1 {
      margin: 0;
      color: var(--green);
      font-size: 15px;
      font-weight: 700;
    }

    .status { color: var(--muted); font-size: 12px; }
    .status.connected { color: var(--green); }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .session-info {
      color: var(--muted);
      font-size: 11px;
      text-align: right;
    }

    #log {
      min-height: 0;
      overflow-y: auto;
      padding: 18px;
      background: var(--terminal);
    }

    .entry {
      margin: 0 0 14px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      line-height: 1.5;
      font-size: 14px;
    }

    .prompt { color: var(--blue); }
    .assistant { color: var(--ink); }
    .event { color: var(--muted); border-left: 2px solid var(--line); padding-left: 10px; }
    .tool { color: var(--amber); border-left-color: var(--amber); }
    .error { color: var(--red); border-left-color: var(--red); }

    .compact-card {
      display: grid;
      gap: 8px;
      margin: 0 0 14px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(17, 27, 34, 0.68);
    }

    .compact-card.tool-group {
      border-color: rgba(242, 184, 75, 0.45);
      background: rgba(242, 184, 75, 0.06);
    }

    .card-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      color: var(--ink);
      font-weight: 700;
      font-size: 13px;
    }

    .card-meta {
      color: var(--muted);
      font-size: 12px;
      font-weight: 400;
    }

    .event-row {
      display: grid;
      gap: 4px;
      padding: 8px 0 0;
      border-top: 1px solid rgba(39, 65, 58, 0.65);
    }

    .event-row:first-of-type { border-top: 0; padding-top: 0; }

    .event-name {
      color: var(--amber);
      font-size: 12px;
      font-weight: 700;
    }

    .event-summary {
      color: var(--ink);
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    details.payload {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }

    details.payload summary {
      cursor: pointer;
      user-select: none;
    }

    details.payload pre {
      max-height: 300px;
      overflow: auto;
      margin: 8px 0 0;
      padding: 8px;
      border: 1px solid rgba(39, 65, 58, 0.7);
      border-radius: 8px;
      color: var(--ink);
      background: rgba(7, 16, 20, 0.78);
      font-size: 12px;
    }

    .turn {
      display: grid;
      gap: 8px;
      margin: 0 0 16px;
      padding-left: 10px;
      border-left: 1px solid rgba(39, 65, 58, 0.7);
    }

    .turn-status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      width: fit-content;
      padding: 4px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      background: rgba(17, 27, 34, 0.8);
      font-size: 12px;
    }

    .turn-status::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--muted);
    }

    .turn-status.running {
      color: var(--green);
      border-color: rgba(71, 209, 140, 0.4);
    }

    .turn-status.running::before {
      background: var(--green);
      box-shadow: 0 0 0 0 rgba(71, 209, 140, 0.5);
      animation: pulse 1.2s infinite;
    }

    .turn-status.waiting {
      color: var(--amber);
      border-color: rgba(242, 184, 75, 0.5);
    }

    .turn-status.waiting::before { background: var(--amber); }

    .turn-status.complete {
      color: var(--green);
      border-color: rgba(71, 209, 140, 0.35);
    }

    .turn-status.complete::before { background: var(--green); }

    .turn-status.failed {
      color: var(--red);
      border-color: rgba(255, 107, 95, 0.55);
    }

    .turn-status.failed::before { background: var(--red); }

    @keyframes pulse {
      70% { box-shadow: 0 0 0 8px rgba(71, 209, 140, 0); }
      100% { box-shadow: 0 0 0 0 rgba(71, 209, 140, 0); }
    }

    .approval {
      display: grid;
      gap: 10px;
      margin: 0 0 14px;
      padding: 12px;
      border: 1px solid rgba(242, 184, 75, 0.6);
      border-radius: 10px;
      background: rgba(242, 184, 75, 0.08);
    }

    .approval-title { color: var(--amber); font-weight: 700; }
    .approval-intention {
      color: var(--ink);
      line-height: 1.45;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .approval pre {
      max-height: 240px;
      overflow: auto;
      margin: 0;
      color: var(--ink);
      font-size: 12px;
    }

    .actions { display: flex; gap: 8px; }

    form {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      min-height: 0;
      padding: 12px;
      border-top: 1px solid var(--line);
      background: rgba(17, 27, 34, 0.96);
    }

    textarea {
      min-height: 54px;
      max-height: 180px;
      resize: vertical;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px;
      color: var(--ink);
      background: #071014;
      font: inherit;
      outline: none;
    }

    textarea:focus { border-color: var(--green); }

    button {
      min-width: 92px;
      border: 1px solid var(--green);
      border-radius: 10px;
      color: #071014;
      background: var(--green);
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }

    button.secondary {
      color: var(--ink);
      border-color: var(--line);
      background: transparent;
    }

    button.danger {
      color: #fff;
      border-color: var(--red);
      background: var(--red);
    }

    button:disabled { opacity: 0.5; cursor: not-allowed; }

    @media (max-width: 700px) {
      main { width: 100vw; height: 100dvh; margin: 0; border: 0; border-radius: 0; }
      header { align-items: flex-start; flex-direction: column; }
      .header-actions { width: 100%; justify-content: space-between; }
      form { grid-template-columns: 1fr; }
      button { min-height: 48px; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>github-copilot-agent</h1>
      <div class="header-actions">
        <div id="sessionInfo" class="session-info"></div>
        <button id="newSession" type="button" class="secondary">New session</button>
        <div id="status" class="status">connecting</div>
      </div>
    </header>
    <section id="log" aria-live="polite"></section>
    <form id="form">
      <textarea id="input" placeholder="Ask Copilot to inspect, explain, or change code..."></textarea>
      <button id="send" disabled>Run</button>
    </form>
  </main>

  <script>
    const log = document.getElementById("log");
    const form = document.getElementById("form");
    const input = document.getElementById("input");
    const send = document.getElementById("send");
    const newSession = document.getElementById("newSession");
    const statusText = document.getElementById("status");
    const sessionInfo = document.getElementById("sessionInfo");

    const userId = localStorage.getItem("copilot:userId") || `copilot-web-${crypto.randomUUID()}`;
    let conversationId = localStorage.getItem("copilot:conversationId") || newConversationId();
    let agentSessionId = localStorage.getItem("copilot:agentSessionId") || newAgentSessionId();
    localStorage.setItem("copilot:userId", userId);
    localStorage.setItem("copilot:conversationId", conversationId);
    localStorage.setItem("copilot:agentSessionId", agentSessionId);

    const streams = new Map();
    let socket;

    updateSessionInfo();

    function newConversationId() {
      return `copilot-session-${crypto.randomUUID()}`;
    }

    function newAgentSessionId() {
      return `session-${crypto.randomUUID()}`;
    }

    function startNewSession() {
      conversationId = newConversationId();
      agentSessionId = newAgentSessionId();
      localStorage.setItem("copilot:conversationId", conversationId);
      localStorage.setItem("copilot:agentSessionId", agentSessionId);
      updateSessionInfo();
      streams.clear();
      log.replaceChildren();
      append("event", `Started new session\nconversation_id=${conversationId}\nagent_session_id=${agentSessionId}`);
      input.focus();
    }

    function updateSessionInfo() {
      sessionInfo.textContent = `session ${shortId(agentSessionId)} / conversation ${shortId(conversationId)}`;
      sessionInfo.title = `agent_session_id=${agentSessionId}\nconversation_id=${conversationId}\nuser_id=${userId}`;
    }

    function setStatus(text, connected = false) {
      statusText.textContent = text;
      statusText.classList.toggle("connected", connected);
      send.disabled = !connected;
    }

    function append(className, text) {
      const node = document.createElement("pre");
      node.className = `entry ${className}`;
      node.textContent = text;
      log.append(node);
      log.scrollTop = log.scrollHeight;
      return node;
    }

    function appendToTurn(stream, className, text) {
      const node = document.createElement("pre");
      node.className = `entry ${className}`;
      node.textContent = text;
      stream.events.append(node);
      moveStatusToEnd(stream);
      log.scrollTop = log.scrollHeight;
      return node;
    }

    function createAssistantTurn() {
      const turn = document.createElement("section");
      turn.className = "turn";

      const events = document.createElement("div");
      events.className = "turn-events";

      const status = document.createElement("div");
      status.className = "turn-status running";
      status.textContent = "Running";

      turn.append(events, status);
      log.append(turn);
      log.scrollTop = log.scrollHeight;

      return { turn, events, status, currentMessage: null, toolGroups: new Map() };
    }

    function ensureMessageNode(stream) {
      if (!stream.currentMessage) {
        stream.currentMessage = appendToTurn(stream, "assistant", "");
      }
      return stream.currentMessage;
    }

    function finishMessageNode(stream) {
      stream.currentMessage = null;
    }

    function moveStatusToEnd(stream) {
      stream.turn.append(stream.status);
    }

    function setTurnStatus(stream, state, text) {
      stream.status.className = `turn-status ${state}`;
      stream.status.textContent = text;
      moveStatusToEnd(stream);
      log.scrollTop = log.scrollHeight;
    }

    function parseMessage(raw) {
      const message = JSON.parse(raw);
      if (message.type !== "message") {
        return null;
      }
      if (message.from === "group" || message.from === "server") {
        return typeof message.data === "string" ? JSON.parse(message.data) : message.data;
      }
      return null;
    }

    async function connect() {
      setStatus("connecting");
      const response = await fetch(`/negotiate?user_id=${encodeURIComponent(userId)}`);
      if (!response.ok) throw new Error(await response.text());
      const { url } = await response.json();
      socket = new WebSocket(url, "json.webpubsub.azure.v1");
      socket.addEventListener("open", () => setStatus("connected", true));
      socket.addEventListener("close", () => {
        setStatus("disconnected");
        setTimeout(() => connect().catch(showError), 1500);
      });
      socket.addEventListener("message", event => {
        const data = parseMessage(event.data);
        if (data) handleStreamEvent(data);
      });
    }

    function handleStreamEvent(event) {
      const stream = streams.get(event.stream_id);
      if (!stream) return;

      if (event.type === "text.delta") {
        ensureMessageNode(stream).textContent += event.data?.text || "";
        setTurnStatus(stream, "running", "Streaming");
      } else if (event.type === "message.final") {
        ensureMessageNode(stream).textContent = event.data?.content || ensureMessageNode(stream).textContent;
        finishMessageNode(stream);
        setTurnStatus(stream, "running", "Finalizing");
      } else if (event.type === "approval.requested") {
        finishMessageNode(stream);
        setTurnStatus(stream, "waiting", "Waiting for approval");
        renderApproval(event, stream);
      } else if (event.type === "approval.waiting") {
        setTurnStatus(stream, "waiting", "Waiting for approval");
      } else if (event.type === "approval.received") {
        setTurnStatus(stream, "running", "Approval received, resuming");
      } else if (event.type === "approval.timeout") {
        setTurnStatus(stream, "failed", "Approval timed out");
      } else if (event.type === "tool.event") {
        finishMessageNode(stream);
        if (isActiveToolEvent(event.data?.source_type)) {
          setTurnStatus(stream, "running", "Running tool");
        }
        renderToolEvent(event, stream);
      } else if (event.type === "copilot.event" || event.type === "copilot.status") {
        finishMessageNode(stream);
        renderGenericEvent(event, stream);
      } else if (event.type === "error" || event.type === "copilot.error") {
        finishMessageNode(stream);
        setTurnStatus(stream, "failed", "Failed");
        renderGenericEvent(event, stream, "error");
      } else if (event.type === "done") {
        finishMessageNode(stream);
        setTurnStatus(stream, "complete", "Turn complete");
        streams.delete(event.stream_id);
      }
      log.scrollTop = log.scrollHeight;
    }

    function isActiveToolEvent(sourceType) {
      const value = String(sourceType || "").toLowerCase();
      return value.includes("tool.call") ||
        value.includes("tool_call") ||
        value.includes("tool.result") ||
        value.includes("tool_result") ||
        value.includes("external_tool");
    }

    function renderToolEvent(event, stream) {
      const payload = event.data?.payload || {};
      const toolId = getToolCallId(payload) || getToolCallId(event.data) || `ungrouped-${event.sequence || crypto.randomUUID()}`;
      const group = ensureToolGroup(stream, toolId, payload, event.data?.source_type);
      const row = createEventRow(
        toolEventLabel(event.data?.source_type),
        summarizeToolEvent(event.data?.source_type, payload),
        payload,
        { collapsePayload: isToolStartEvent(event.data?.source_type) }
      );
      group.body.append(row);
      updateToolGroupTitle(group, payload, event.data?.source_type);
      moveStatusToEnd(stream);
      log.scrollTop = log.scrollHeight;
    }

    function ensureToolGroup(stream, toolId, payload, sourceType) {
      if (stream.toolGroups.has(toolId)) {
        return stream.toolGroups.get(toolId);
      }

      const card = document.createElement("section");
      card.className = "compact-card tool-group";

      const title = document.createElement("div");
      title.className = "card-title";

      const label = document.createElement("span");
      label.textContent = toolTitle(payload, sourceType);

      const meta = document.createElement("span");
      meta.className = "card-meta";
      meta.textContent = shortId(toolId);

      const body = document.createElement("div");
      body.className = "tool-events";

      title.append(label, meta);
      card.append(title, body);
      stream.events.append(card);
      moveStatusToEnd(stream);

      const group = { card, title, label, meta, body, toolId };
      stream.toolGroups.set(toolId, group);
      return group;
    }

    function updateToolGroupTitle(group, payload, sourceType) {
      group.label.textContent = toolTitle(payload, sourceType);
    }

    function renderGenericEvent(event, stream, variant = "") {
      const payload = event.data || {};
      const node = document.createElement("section");
      node.className = `compact-card ${variant}`;
      node.append(createEventRow(
        event.type,
        summarizePayload(payload),
        payload,
        { collapsePayload: true }
      ));
      stream.events.append(node);
      moveStatusToEnd(stream);
      log.scrollTop = log.scrollHeight;
    }

    function createEventRow(name, summary, payload, options = {}) {
      const row = document.createElement("div");
      row.className = "event-row";

      const eventName = document.createElement("div");
      eventName.className = "event-name";
      eventName.textContent = name || "event";

      const eventSummary = document.createElement("div");
      eventSummary.className = "event-summary";
      eventSummary.textContent = summary || "No summary";

      row.append(eventName, eventSummary);

      const details = createPayloadDetails(payload, options);
      if (details) {
        row.append(details);
      }
      return row;
    }

    function createPayloadDetails(payload, options = {}) {
      const json = JSON.stringify(payload || {}, null, 2);
      if (!json || json === "{}") {
        return null;
      }

      const details = document.createElement("details");
      details.className = "payload";
      details.open = !options.collapsePayload && json.length < 700;

      const summary = document.createElement("summary");
      summary.textContent = json.length < 700 ? "Payload" : `Payload collapsed (${json.length} chars)`;

      const pre = document.createElement("pre");
      pre.textContent = json;

      details.append(summary, pre);
      return details;
    }

    function getToolCallId(payload) {
      return findFirst(payload, [
        "tool_call_id",
        "toolCallId",
        "toolCallID",
        "interaction_id",
        "interactionId",
        "request_id",
        "requestId",
        "id"
      ]);
    }

    function toolTitle(payload, sourceType) {
      const name = findFirst(payload, ["tool_name", "toolName", "name", "command_name", "commandName"]) || "Tool call";
      const status = findFirst(payload, ["status", "result_type", "resultType"]);
      return status ? `${name} (${status})` : `${name} (${sourceType || "tool"})`;
    }

    function summarizePayload(payload) {
      const message = findFirst(payload, ["message", "content", "error"]);
      if (typeof message === "string" && message.trim()) {
        return truncate(message.trim(), 240);
      }

      const command = findFirst(payload, ["command", "full_command_text", "fullCommandText"]);
      if (typeof command === "string" && command.trim()) {
        return truncate(command.trim(), 240);
      }

      const toolName = findFirst(payload, ["tool_name", "toolName", "name", "command_name", "commandName"]);
      const status = findFirst(payload, ["status", "result_type", "resultType"]);
      if (toolName || status) {
        return [toolName, status].filter(Boolean).join(" - ");
      }

      return truncate(JSON.stringify(payload || {}), 240);
    }

    function toolEventLabel(sourceType) {
      const value = String(sourceType || "").toLowerCase();
      if (value.includes("start") || value.includes("requested") || value.includes("call")) {
        return "Tool start";
      }
      if (value.includes("complete") || value.includes("result")) {
        return "Tool complete";
      }
      return sourceType || "Tool event";
    }

    function summarizeToolEvent(sourceType, payload) {
      if (isToolStartEvent(sourceType)) {
        return `${toolName(payload)}\n${formatToolArguments(payload)}`;
      }
      return summarizePayload(payload);
    }

    function isToolStartEvent(sourceType) {
      const value = String(sourceType || "").toLowerCase();
      return value.includes("start") ||
        value.includes("requested") ||
        value.includes("tool.call") ||
        value.includes("tool_call") ||
        value.includes("external_tool");
    }

    function toolName(payload) {
      return findFirst(payload, ["tool_name", "toolName", "name", "command_name", "commandName"]) || "Tool";
    }

    function formatToolArguments(payload) {
      const args = findFirst(payload, ["arguments", "args", "input", "parameters", "params"]);
      if (!args) {
        const command = findFirst(payload, ["command", "full_command_text", "fullCommandText"]);
        return command ? `command: ${command}` : "No arguments";
      }
      if (typeof args === "string") {
        return truncate(args, 400);
      }
      if (typeof args !== "object") {
        return String(args);
      }
      return Object.entries(args)
        .map(([key, value]) => `${key}: ${formatValue(value)}`)
        .join("\n") || "No arguments";
    }

    function formatValue(value) {
      if (value === null || value === undefined) {
        return "";
      }
      if (typeof value === "string") {
        return value;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        return String(value);
      }
      return truncate(JSON.stringify(value), 260);
    }

    function approvalIntention(details) {
      const explicit = findOwn(details, ["intention"]);
      if (explicit) {
        return explicit;
      }

      const request = details?.request || details?.permissionRequest || details || {};
      const requestIntention = findOwn(request, ["intention"]);
      if (requestIntention) {
        return requestIntention;
      }

      const command = findFirst(request, ["full_command_text", "fullCommandText", "command"]);
      if (command) {
        return `Allow Copilot to run:\n${command}`;
      }

      const path = findFirst(request, ["path", "file", "target"]);
      const operation = findFirst(request, ["operation", "kind", "type", "name"]) || "perform this action";
      if (path) {
        return `Allow Copilot to ${operation} on ${path}`;
      }

      const tool = findFirst(request, ["tool_name", "toolName", "name"]);
      if (tool) {
        return `Allow Copilot to use ${tool}`;
      }

      return "Allow Copilot to perform the requested action";
    }

    function findOwn(value, keys) {
      if (!value || typeof value !== "object") {
        return null;
      }
      for (const key of keys) {
        if (value[key] !== undefined && value[key] !== null && value[key] !== "") {
          return value[key];
        }
      }
      return null;
    }

    function findFirst(value, keys) {
      if (!value || typeof value !== "object") {
        return null;
      }
      for (const key of keys) {
        if (value[key] !== undefined && value[key] !== null) {
          return value[key];
        }
      }
      for (const child of Object.values(value)) {
        if (child && typeof child === "object") {
          const found = findFirst(child, keys);
          if (found !== null && found !== undefined) {
            return found;
          }
        }
      }
      return null;
    }

    function shortId(value) {
      const text = String(value || "");
      if (text.length <= 18) {
        return text;
      }
      return `${text.slice(0, 8)}...${text.slice(-6)}`;
    }

    function truncate(value, maxLength) {
      const text = String(value || "");
      return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
    }

    function renderApproval(event, stream) {
      const card = document.createElement("div");
      card.className = "approval";

      const title = document.createElement("div");
      title.className = "approval-title";
      title.textContent = "Approval required";

      const intention = document.createElement("div");
      intention.className = "approval-intention";
      intention.textContent = event.data?.intention || approvalIntention(event.data?.details || {});

      const details = createPayloadDetails(
        event.data?.details || {},
        { collapsePayload: true }
      );

      const actions = document.createElement("div");
      actions.className = "actions";

      const approve = document.createElement("button");
      approve.type = "button";
      approve.textContent = "Approve";

      const deny = document.createElement("button");
      deny.type = "button";
      deny.textContent = "Deny";
      deny.className = "danger";

      approve.onclick = () => resolveApproval(event, "approved", card);
      deny.onclick = () => resolveApproval(event, "denied", card);

      actions.append(approve, deny);
      card.append(title, intention);
      if (details) {
        card.append(details);
      }
      card.append(actions);
      const toolId = getToolCallId(event.data?.details);
      if (toolId) {
        const group = ensureToolGroup(
          stream,
          toolId,
          event.data?.details || {},
          "permission.requested"
        );
        group.body.append(card);
      } else {
        stream.events.append(card);
      }
      moveStatusToEnd(stream);
      log.scrollTop = log.scrollHeight;
      stream.approvalCard = card;
    }

    function resolveApproval(event, decision, card) {
      const stream = streams.get(event.stream_id);
      socket.send(JSON.stringify({
        type: "sendToGroup",
        group: `stream-${event.stream_id}`,
        dataType: "json",
        data: {
          type: "approval.resolved",
          stream_id: event.stream_id,
          request_id: event.request_id,
          conversation_id: event.conversation_id,
          data: {
            approval_id: event.data?.approval_id,
            decision
          }
        },
        ackId: Date.now()
      }));
      card.querySelector(".actions").replaceWith(document.createTextNode(`Decision: ${decision}`));
      if (stream) {
        setTurnStatus(stream, "running", decision === "approved" ? "Approval sent, running" : "Denied, waiting for agent");
      }
    }

    function joinStreamGroup(streamId) {
      socket.send(JSON.stringify({
        type: "joinGroup",
        group: `stream-${streamId}`,
        ackId: Date.now()
      }));
    }

    function showError(error) {
      setStatus("error");
      append("event error", error.message || String(error));
    }

    form.addEventListener("submit", async event => {
      event.preventDefault();
      const message = input.value.trim();
      if (!message || socket?.readyState !== WebSocket.OPEN) return;

      input.value = "";
      append("prompt", `$ ${message}`);

      const streamId = `stream-${crypto.randomUUID()}`;
      joinStreamGroup(streamId);
      streams.set(streamId, createAssistantTurn());

      try {
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
        if (!response.ok) throw new Error(await response.text());
      } catch (error) {
        const stream = streams.get(streamId);
        if (stream) {
          setTurnStatus(stream, "failed", "Request failed");
        }
        streams.delete(streamId);
        append("event error", error.message || String(error));
      }
    });

    input.addEventListener("keydown", event => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        form.requestSubmit();
      }
    });

    newSession.addEventListener("click", startNewSession);

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
        raise RuntimeError("WEBPUBSUB_CONNECTION_STRING is required for the Copilot web UI.")

    service = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub)
    credential = DefaultAzureCredential()
    auth_scope = os.getenv("FOUNDRY_AGENT_AUTH_SCOPE", "https://ai.azure.com/.default")
    should_auth_agent = _should_auth_agent_url(agent_url)

    async def index(_: Request) -> HTMLResponse:
        return HTMLResponse(HTML)

    async def negotiate(request: Request) -> JSONResponse:
        user_id = request.query_params.get("user_id") or f"copilot-web-{uuid.uuid4()}"
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
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument(
        "--agent-url",
        default=os.getenv("COPILOT_AGENT_INVOCATION_URL", "http://localhost:8088/invocations"),
    )
    args = parser.parse_args()

    config = Config()
    config.bind = [f"{args.host}:{args.port}"]

    print(f"Copilot Web UI: http://{args.host}:{args.port}")
    print(f"Agent invocation URL: {args.agent_url}")
    asyncio.run(serve(create_app(args.agent_url), config))


if __name__ == "__main__":
    main()
