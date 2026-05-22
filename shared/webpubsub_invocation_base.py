from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol

from azure.ai.agentserver.invocations import InvocationAgentServerHost
from azure.messaging.webpubsubclient.aio import WebPubSubClient
from azure.messaging.webpubsubclient.models import (
    CallbackType,
    WebPubSubDataType,
    WebPubSubProtocolType,
)
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


def log_debug(message: str, **fields: Any) -> None:
    if os.getenv("COPILOT_AGENT_DEBUG_LOGS", "false").lower() not in {"1", "true", "yes"}:
        return

    safe_fields = {
        key: _redact(value)
        for key, value in fields.items()
    }
    print(
        json.dumps(
            {
                "component": "hosted-agent-wps",
                "message": message,
                **safe_fields,
            },
            default=str,
        ),
        flush=True,
    )


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("<redacted>" if _looks_secret_key(str(key)) else _redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str) and _looks_secret_value(value):
        return "<redacted>"
    return value


def _looks_secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in ("token", "key", "secret", "connection_string"))


def _looks_secret_value(value: str) -> bool:
    return (
        "AccessKey=" in value
        or value.startswith("github_pat_")
        or value.startswith("ghp_")
        or len(value) > 120 and "." in value
    )


@dataclass(frozen=True)
class InvocationContext:
    request_id: str
    stream_id: str
    user_id: str
    conversation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamEvent:
    type: str
    data: dict[str, Any] = field(default_factory=dict)


StreamFactory = Callable[["WebPubSubRunContext"], AsyncIterator[StreamEvent]]


class StreamTransport(Protocol):
    async def publish(self, context: InvocationContext, sequence: int, event: StreamEvent) -> None:
        ...

    async def close(self) -> None:
        ...


class BidirectionalStreamTransport(StreamTransport, Protocol):
    async def receive(self, timeout: float | None = None) -> dict[str, Any]:
        ...


class ConsoleTransport:
    async def publish(self, context: InvocationContext, sequence: int, event: StreamEvent) -> None:
        envelope = build_event_envelope(context, sequence, event)
        print(json.dumps(envelope), flush=True)

    async def close(self) -> None:
        return None


class WebPubSubTransport:
    def __init__(self, connection_string: str, hub: str) -> None:
        self._client = WebPubSubServiceClient.from_connection_string(
            connection_string,
            hub=hub,
        )

    async def publish(self, context: InvocationContext, sequence: int, event: StreamEvent) -> None:
        envelope = build_event_envelope(context, sequence, event)
        await asyncio.to_thread(
            self._client.send_to_user,
            user_id=context.user_id,
            message=envelope,
            content_type="application/json",
        )

    async def close(self) -> None:
        return None


class WebPubSubClientConnectionTransport:
    def __init__(self, connection_string: str, hub: str, user_id: str) -> None:
        service = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub)
        token = service.get_client_access_token(
            user_id=user_id,
            roles=["webpubsub.joinLeaveGroup", "webpubsub.sendToGroup"],
        )
        self._client = WebPubSubClient(
            token["url"],
            protocol_type=WebPubSubProtocolType.JSON,
        )
        self._open_lock = asyncio.Lock()
        self._opened = False
        self._joined_groups: set[str] = set()
        self._inbound_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        log_debug(
            "webpubsub_client_transport_created",
            hub=hub,
            user_id=user_id,
        )

    async def publish(self, context: InvocationContext, sequence: int, event: StreamEvent) -> None:
        await self._ensure_open()
        group_name = self._group_name(context)
        if group_name not in self._joined_groups:
            await self._client.join_group(group_name)
            self._joined_groups.add(group_name)
            log_debug(
                "webpubsub_agent_joined_group",
                group=group_name,
                stream_id=context.stream_id,
            )

        log_debug(
            "webpubsub_publish_group",
            group=group_name,
            event_type=event.type,
            stream_id=context.stream_id,
            sequence=sequence,
        )
        await self._client.send_to_group(
            group_name,
            build_event_envelope(context, sequence, event),
            WebPubSubDataType.JSON,
            no_echo=True,
        )

    async def receive(self, timeout: float | None = None) -> dict[str, Any]:
        await self._ensure_open()
        log_debug("webpubsub_receive_wait_start", timeout=timeout)
        if timeout is None:
            event = await self._inbound_queue.get()
        else:
            event = await asyncio.wait_for(self._inbound_queue.get(), timeout=timeout)
        log_debug(
            "webpubsub_receive_wait_end",
            event_type=event.get("type"),
            stream_id=event.get("stream_id"),
            data_keys=list((event.get("data") or {}).keys()) if isinstance(event.get("data"), dict) else None,
        )
        return event

    async def close(self) -> None:
        if self._opened:
            await self._client.close()
            self._opened = False

    async def _ensure_open(self) -> None:
        async with self._open_lock:
            if self._opened:
                return

            await self._client.subscribe(CallbackType.GROUP_MESSAGE, self._on_group_message)
            await self._client.open()
            self._opened = True
            log_debug("webpubsub_client_opened")

    async def _on_group_message(self, event) -> None:
        data = event.data
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        if isinstance(data, str):
            data = json.loads(data)
        if isinstance(data, dict):
            log_debug(
                "webpubsub_group_message_received",
                event_type=data.get("type"),
                stream_id=data.get("stream_id"),
                data_keys=list((data.get("data") or {}).keys()) if isinstance(data.get("data"), dict) else None,
            )
            await self._inbound_queue.put(data)

    def _group_name(self, context: InvocationContext) -> str:
        return f"stream-{context.stream_id}"


class WebPubSubRunContext:
    def __init__(
        self,
        invocation: InvocationContext,
        transport: StreamTransport,
    ) -> None:
        self.invocation = invocation
        self._transport = transport

    @property
    def request_id(self) -> str:
        return self.invocation.request_id

    @property
    def stream_id(self) -> str:
        return self.invocation.stream_id

    @property
    def conversation_id(self) -> str | None:
        return self.invocation.conversation_id

    @property
    def user_id(self) -> str:
        return self.invocation.user_id

    def text_delta(self, text: str) -> StreamEvent:
        return StreamEvent(type="text.delta", data={"text": text})

    def final_message(self, content: str, role: str = "assistant") -> StreamEvent:
        return StreamEvent(
            type="message.final",
            data={
                "role": role,
                "content": content,
            },
        )

    def event(self, event_type: str, **data: Any) -> StreamEvent:
        return StreamEvent(type=event_type, data=data)

    async def publish_event(self, event: StreamEvent, sequence: int = 0) -> None:
        log_debug(
            "run_context_publish_event",
            event_type=event.type,
            stream_id=self.stream_id,
            sequence=sequence,
        )
        await self._transport.publish(self.invocation, sequence, event)

    async def wait_for_event(
        self,
        event_type: str,
        *,
        match: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        if not hasattr(self._transport, "receive"):
            raise RuntimeError(
                "wait_for_event requires WEBPUBSUB_TRANSPORT=client."
            )

        transport = self._transport
        while True:
            inbound = await transport.receive(timeout=timeout)  # type: ignore[attr-defined]
            log_debug(
                "wait_for_event_candidate",
                expected_type=event_type,
                inbound_type=inbound.get("type"),
                expected_stream_id=self.stream_id,
                inbound_stream_id=inbound.get("stream_id"),
                match=match,
            )
            if inbound.get("type") != event_type:
                continue
            if inbound.get("stream_id") not in {None, self.stream_id}:
                continue

            data = inbound.get("data") or {}
            if isinstance(data, str):
                data = json.loads(data)
            if match and any(data.get(key) != value for key, value in match.items()):
                continue

            return inbound

    async def request_approval(
        self,
        *,
        title: str,
        details: dict[str, Any] | None = None,
        approval_id: str | None = None,
        timeout: float | None = None,
    ) -> tuple[StreamEvent, Awaitable[dict[str, Any]]]:
        approval_id = approval_id or str(uuid.uuid4())
        event = StreamEvent(
            type="approval.requested",
            data={
                "approval_id": approval_id,
                "title": title,
                "details": details or {},
            },
        )
        waiter = self.wait_for_event(
            "approval.resolved",
            match={"approval_id": approval_id},
            timeout=timeout,
        )
        return event, waiter


def build_event_envelope(
    context: InvocationContext,
    sequence: int,
    event: StreamEvent,
) -> dict[str, Any]:
    return {
        "type": event.type,
        "request_id": context.request_id,
        "stream_id": context.stream_id,
        "conversation_id": context.conversation_id,
        "sequence": sequence,
        "data": event.data,
    }


def create_transport_from_env() -> StreamTransport:
    if os.getenv("MOCK_WEBPUBSUB", "").lower() in {"1", "true", "yes"}:
        return ConsoleTransport()

    connection_string = os.getenv("WEBPUBSUB_CONNECTION_STRING")
    hub = os.getenv("WEBPUBSUB_HUB", "chat")
    if not connection_string:
        raise RuntimeError(
            "WEBPUBSUB_CONNECTION_STRING is required unless MOCK_WEBPUBSUB=true."
        )

    mode = os.getenv("WEBPUBSUB_TRANSPORT", "service").lower()
    if mode == "client":
        return WebPubSubClientConnectionTransport(
            connection_string=connection_string,
            hub=hub,
            user_id=os.getenv("WEBPUBSUB_AGENT_USER_ID", "hosted-agent"),
        )

    return WebPubSubTransport(connection_string=connection_string, hub=hub)


class WebPubSubInvocationHandler:
    def __init__(self, transport: StreamTransport) -> None:
        self._transport = transport

    async def handle(self, request: Request) -> Response:
        try:
            body = await request.json()
            context = self._context_from_body(body)
        except Exception as exc:
            return JSONResponse(
                {"status": "rejected", "error": str(exc)},
                status_code=400,
            )

        asyncio.create_task(self._publish_stream(body, context))

        return JSONResponse(
            {
                "status": "streaming",
                "request_id": context.request_id,
                "stream_id": context.stream_id,
                "conversation_id": context.conversation_id,
                "delivery": {
                    "type": "webpubsub",
                    "user_id": context.user_id,
                },
            },
            status_code=202,
        )

    async def stream_events(
        self,
        body: dict[str, Any],
        context: InvocationContext,
    ) -> AsyncIterator[StreamEvent]:
        raise NotImplementedError

    def _context_from_body(self, body: dict[str, Any]) -> InvocationContext:
        user_id = str(body.get("user_id") or "").strip()
        if not user_id:
            raise ValueError("Request body must include user_id.")

        return InvocationContext(
            request_id=str(body.get("request_id") or uuid.uuid4()),
            stream_id=str(body.get("stream_id") or uuid.uuid4()),
            user_id=user_id,
            conversation_id=body.get("conversation_id"),
            metadata=dict(body.get("metadata") or {}),
        )

    async def _publish_stream(
        self,
        body: dict[str, Any],
        context: InvocationContext,
    ) -> None:
        sequence = 0
        try:
            async for event in self.stream_events(body, context):
                sequence += 1
                await self._transport.publish(context, sequence, event)

            sequence += 1
            await self._transport.publish(context, sequence, StreamEvent(type="done"))
        except Exception as exc:
            sequence += 1
            await self._transport.publish(
                context,
                sequence,
                StreamEvent(type="error", data={"message": str(exc)}),
            )


class WebPubSubStreamingResponse:
    def __init__(
        self,
        content: AsyncIterator[StreamEvent] | StreamFactory,
        *,
        user_id: str,
        request_id: str | None = None,
        stream_id: str | None = None,
        conversation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        status_code: int = 202,
    ) -> None:
        self.content = content
        self.context = InvocationContext(
            request_id=request_id or str(uuid.uuid4()),
            stream_id=stream_id or str(uuid.uuid4()),
            user_id=user_id,
            conversation_id=conversation_id,
            metadata=metadata or {},
        )
        self.status_code = status_code

    def create_stream(self, context: WebPubSubRunContext) -> AsyncIterator[StreamEvent]:
        if callable(self.content):
            return self.content(context)
        return self.content


class WebPubSubInvocationAgentServerHost(InvocationAgentServerHost):
    def __init__(self, *, transport: StreamTransport | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._webpubsub_transport = transport or create_transport_from_env()

    async def _dispatch_invoke(self, request: Request) -> Response:
        response = await super()._dispatch_invoke(request)
        if isinstance(response, WebPubSubStreamingResponse):
            asyncio.create_task(self._publish_streaming_response(response))
            return JSONResponse(
                {
                    "status": "streaming",
                    "request_id": response.context.request_id,
                    "stream_id": response.context.stream_id,
                    "conversation_id": response.context.conversation_id,
                    "delivery": {
                        "type": "webpubsub",
                        "user_id": response.context.user_id,
                    },
                },
                status_code=response.status_code,
            )

        return response

    async def _publish_streaming_response(
        self,
        response: WebPubSubStreamingResponse,
    ) -> None:
        sequence = 0
        run_context = WebPubSubRunContext(
            invocation=response.context,
            transport=self._webpubsub_transport,
        )
        try:
            async for event in response.create_stream(run_context):
                sequence += 1
                await self._webpubsub_transport.publish(response.context, sequence, event)

            sequence += 1
            await self._webpubsub_transport.publish(
                response.context,
                sequence,
                StreamEvent(type="done"),
            )
        except Exception as exc:
            sequence += 1
            await self._webpubsub_transport.publish(
                response.context,
                sequence,
                StreamEvent(type="error", data={"message": str(exc)}),
            )
