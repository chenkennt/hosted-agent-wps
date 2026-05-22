from __future__ import annotations

import asyncio
import dataclasses
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.webpubsub_invocation_base import (
    StreamEvent,
    WebPubSubInvocationAgentServerHost,
    WebPubSubRunContext,
    WebPubSubStreamingResponse,
    log_debug,
)


load_dotenv()

if os.getenv("WEBPUBSUB_TRANSPORT", "service").lower() != "client":
    raise RuntimeError(
        "agents.copilot requires WEBPUBSUB_TRANSPORT=client so approval responses "
        "can be received from Azure Web PubSub."
    )

log_debug(
    "copilot_agent_starting",
    webpubsub_transport=os.getenv("WEBPUBSUB_TRANSPORT"),
    webpubsub_agent_user_id=os.getenv("WEBPUBSUB_AGENT_USER_ID"),
    webpubsub_hub=os.getenv("WEBPUBSUB_HUB"),
    copilot_workdir=os.getenv("COPILOT_WORKDIR"),
    copilot_home=os.getenv("COPILOT_HOME"),
    has_copilot_token=bool(os.getenv("COPILOT_GITHUB_TOKEN")),
)

app = WebPubSubInvocationAgentServerHost(
    openapi_spec={
        "openapi": "3.0.0",
        "info": {
            "title": "GitHub Copilot Web PubSub Invocation Agent",
            "version": "0.1.0",
        },
        "paths": {
            "/invocations": {
                "post": {
                    "summary": "Start a GitHub Copilot coding-agent turn and stream events through Azure Web PubSub.",
                }
            }
        },
    }
)


async def stream_copilot(
    message: str,
    conversation_id: str | None,
    context: WebPubSubRunContext,
) -> AsyncIterator[StreamEvent]:
    try:
        from copilot import CopilotClient, SubprocessConfig
        from copilot.session import PermissionRequestResult
    except ImportError as exc:
        raise RuntimeError(
            "GitHub Copilot SDK is not installed. Run `pip install github-copilot-sdk`."
        ) from exc

    queue: asyncio.Queue[StreamEvent | None] = asyncio.Queue()
    done = asyncio.Event()
    loop = asyncio.get_running_loop()
    final_message: list[str] = []

    def emit(event: StreamEvent) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, event)

    async def on_permission_request(request: Any, invocation: dict[str, Any]) -> Any:
        approval_id = str(uuid.uuid4())
        details = {
            "request": _to_plain(request),
            "invocation": _to_plain(invocation),
        }
        intention = _approval_intention(request)
        log_debug(
            "approval_request_received_from_copilot",
            approval_id=approval_id,
            stream_id=context.stream_id,
            conversation_id=conversation_id,
            intention=intention,
            request_kind=_get_any(request, details["request"], "kind"),
            tool_call_id=_get_any(request, details["request"], "tool_call_id", "toolCallId"),
            invocation=invocation,
        )
        await context.publish_event(
            context.event(
                "approval.requested",
                approval_id=approval_id,
                title="Approval required",
                intention=intention,
                details=details,
            ),
            sequence=-1,
        )
        await context.publish_event(
            context.event("approval.waiting", approval_id=approval_id),
            sequence=-1,
        )
        approval_timeout = _approval_timeout_seconds()
        log_debug(
            "approval_wait_started",
            approval_id=approval_id,
            stream_id=context.stream_id,
            timeout_seconds=approval_timeout,
        )

        try:
            inbound = await context.wait_for_event(
                "approval.resolved",
                match={"approval_id": approval_id},
                timeout=approval_timeout,
            )
        except asyncio.TimeoutError:
            log_debug(
                "approval_wait_timeout",
                approval_id=approval_id,
                stream_id=context.stream_id,
            )
            await queue.put(context.event("approval.timeout", approval_id=approval_id))
            return PermissionRequestResult(kind="reject")
        except Exception as exc:
            log_debug(
                "approval_wait_failed",
                approval_id=approval_id,
                stream_id=context.stream_id,
                error=str(exc),
            )
            await queue.put(
                StreamEvent(
                    type="error",
                    data={"message": f"Permission approval failed: {exc}"},
                )
            )
            return PermissionRequestResult(kind="user-not-available")

        await queue.put(context.event("approval.received", approval_id=approval_id))

        data = inbound.get("data") or {}
        decision = str(data.get("decision") or "").lower()
        log_debug(
            "approval_decision_received",
            approval_id=approval_id,
            stream_id=context.stream_id,
            decision=decision,
            inbound_type=inbound.get("type"),
        )
        if decision in {"approve", "approved", "allow", "allowed", "yes", "true"}:
            log_debug("approval_decision_returning", approval_id=approval_id, result="approve-once")
            return PermissionRequestResult(kind="approve-once")
        log_debug("approval_decision_returning", approval_id=approval_id, result="reject")
        return PermissionRequestResult(kind="reject")

    def on_session_event(event: Any) -> None:
        event_type = _event_type(event)
        data = getattr(event, "data", event)
        payload = _to_plain(data)

        data_type = data.__class__.__name__
        delta = _get_any(data, payload, "delta_content", "deltaContent", "delta")
        content = _get_any(data, payload, "content")

        if data_type in {"AssistantMessageDeltaData", "AssistantReasoningDeltaData"} and delta:
            final_message.append(str(delta))
            emit(context.text_delta(str(delta)))
            return

        if data_type in {"AssistantMessageData", "AssistantReasoningData"} and content:
            joined = str(content)
            final_message[:] = [joined]
            emit(context.final_message(joined))
            return

        if data_type == "SessionIdleData":
            emit(context.event("copilot.status", state="idle"))
            done.set()
            return

        if _should_surface_event(event_type):
            emit(context.event(_map_event_type(event_type), source_type=event_type, payload=payload))

    async def run_copilot() -> None:
        try:
            config = _copilot_subprocess_config(SubprocessConfig)
            log_debug(
                "copilot_client_starting",
                conversation_id=conversation_id,
                workdir=_copilot_workdir(),
                has_config=bool(config),
            )
            async with CopilotClient(config) as client:
                session = await _create_or_resume_session(
                    client,
                    session_id=conversation_id,
                    on_permission_request=on_permission_request,
                )
                async with session:
                    session.on(on_session_event)
                    log_debug(
                        "copilot_session_send",
                        conversation_id=conversation_id,
                        message_length=len(message),
                    )
                    await session.send(message)
                    await done.wait()
        except Exception as exc:
            log_debug("copilot_run_failed", conversation_id=conversation_id, error=str(exc))
            await queue.put(StreamEvent(type="error", data={"message": str(exc)}))
        finally:
            await queue.put(None)

    task = asyncio.create_task(run_copilot())
    try:
        while True:
            event = await queue.get()
            if event is None:
                break
            yield event
    finally:
        if not task.done():
            task.cancel()


async def _create_or_resume_session(
    client: Any,
    *,
    session_id: str | None,
    on_permission_request: Any,
) -> Any:
    workdir = _copilot_workdir()
    kwargs = {
        "on_permission_request": on_permission_request,
        "streaming": True,
    }
    if os.getenv("COPILOT_MODEL"):
        kwargs["model"] = os.getenv("COPILOT_MODEL")
    if workdir:
        kwargs["working_directory"] = workdir
    if os.getenv("COPILOT_AVAILABLE_TOOLS"):
        kwargs["available_tools"] = _split_csv(os.getenv("COPILOT_AVAILABLE_TOOLS"))
    if os.getenv("COPILOT_EXCLUDED_TOOLS"):
        kwargs["excluded_tools"] = _split_csv(os.getenv("COPILOT_EXCLUDED_TOOLS"))
    if session_id and hasattr(client, "resume_session"):
        try:
            return await client.resume_session(session_id, on_permission_request=on_permission_request)
        except Exception as exc:
            if not _is_missing_session_error(exc):
                raise
            pass

    return await client.create_session(**kwargs)


def _is_missing_session_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "session not found" in message or "session.resume failed" in message


def _copilot_subprocess_config(subprocess_config_type: Any) -> Any:
    kwargs: dict[str, Any] = {}
    if _copilot_workdir():
        kwargs["cwd"] = _copilot_workdir()
    if os.getenv("COPILOT_HOME"):
        kwargs["copilot_home"] = os.getenv("COPILOT_HOME")
    if os.getenv("COPILOT_GITHUB_TOKEN"):
        kwargs["github_token"] = os.getenv("COPILOT_GITHUB_TOKEN")

    return subprocess_config_type(**kwargs) if kwargs else None


def _copilot_workdir() -> str | None:
    workdir = os.getenv("COPILOT_WORKDIR")
    if not workdir:
        return os.getcwd()

    resolved = os.path.abspath(workdir)
    if not os.path.isdir(resolved):
        raise RuntimeError(f"COPILOT_WORKDIR does not exist or is not a directory: {resolved}")
    return resolved


def _approval_timeout_seconds() -> float:
    value = os.getenv("COPILOT_APPROVAL_TIMEOUT_SECONDS")
    if not value:
        return 600.0

    try:
        return float(value)
    except ValueError:
        log_debug("invalid_approval_timeout_seconds", value=value, fallback=600.0)
        return 600.0


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _approval_title(request: Any) -> str:
    kind = _get_any(request, _to_plain(request), "kind") or "tool"
    tool_name = _get_any(request, _to_plain(request), "tool_name", "toolName")
    command = _get_any(request, _to_plain(request), "full_command_text", "fullCommandText")
    if command:
        return f"Approve shell command: {command}"
    if tool_name:
        return f"Approve {kind}: {tool_name}"
    return f"Approve {kind}"


def _approval_intention(request: Any) -> str:
    payload = _to_plain(request)
    intention = _get_any(request, payload, "intention")
    if intention:
        return str(intention)

    command = _get_any(request, payload, "full_command_text", "fullCommandText", "command")
    if command:
        return f"Allow Copilot to run:\n{command}"

    tool_title = _get_any(request, payload, "tool_title", "toolTitle")
    if tool_title:
        return f"Allow Copilot to use {tool_title}"

    tool_name = _get_any(request, payload, "tool_name", "toolName")
    if tool_name:
        return f"Allow Copilot to use {tool_name}"

    path = _get_any(request, payload, "path", "file_name", "fileName")
    access_kind = _get_any(request, payload, "access_kind", "accessKind", "kind")
    if path:
        return f"Allow Copilot to {access_kind or 'access'} {path}"

    return "Allow Copilot to perform the requested action"


def _should_surface_event(event_type: str) -> bool:
    lowered = event_type.lower()
    return any(part in lowered for part in ("tool", "permission", "session.", "hook", "error"))


def _map_event_type(event_type: str) -> str:
    lowered = event_type.lower()
    if lowered in {"session.tools_updated", "session_tools_updated"}:
        return "copilot.event"
    if "tool" in lowered:
        return "tool.event"
    if "permission" in lowered:
        return "approval.event"
    if "error" in lowered:
        return "copilot.error"
    return "copilot.event"


def _event_type(event: Any) -> str:
    event_type = str(getattr(event, "type", "") or "")
    if event_type:
        return event_type

    data = getattr(event, "data", event)
    class_name = data.__class__.__name__
    return _camel_to_event_type(class_name)


def _camel_to_event_type(class_name: str) -> str:
    if class_name.endswith("Data"):
        class_name = class_name[:-4]
    chars: list[str] = []
    for index, char in enumerate(class_name):
        if char.isupper() and index > 0:
            chars.append("_")
        chars.append(char.lower())
    return "".join(chars)


def _get_any(source: Any, payload: Any, *names: str) -> Any:
    for name in names:
        if hasattr(source, name):
            return getattr(source, name)
        if isinstance(payload, dict) and name in payload:
            return payload[name]
    return None


def _to_plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(item) for item in value]
    if dataclasses.is_dataclass(value):
        return _to_plain(dataclasses.asdict(value))
    if hasattr(value, "model_dump"):
        return _to_plain(value.model_dump())
    if hasattr(value, "__dict__"):
        return {
            key: _to_plain(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


@app.invoke_handler
async def invoke(request: Request):
    body: dict[str, Any] = await request.json()
    message = str(body.get("message") or "").strip()
    user_id = str(body.get("user_id") or "").strip()
    conversation_id = body.get("conversation_id")

    if not message:
        return JSONResponse(
            {"status": "rejected", "error": "Request body must include message."},
            status_code=400,
        )
    if not user_id:
        return JSONResponse(
            {"status": "rejected", "error": "Request body must include user_id."},
            status_code=400,
        )

    return WebPubSubStreamingResponse(
        lambda run_context: stream_copilot(message, conversation_id, run_context),
        user_id=user_id,
        request_id=body.get("request_id"),
        stream_id=body.get("stream_id"),
        conversation_id=conversation_id,
        metadata=body.get("metadata"),
    )


if __name__ == "__main__":
    app.run()
