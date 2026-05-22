from __future__ import annotations

from typing import Any, AsyncIterator

from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.azure_openai_chat import stream_chat_completion
from shared.chat_history import chat_history
from shared.webpubsub_invocation_base import (
    StreamEvent,
    WebPubSubInvocationAgentServerHost,
    WebPubSubRunContext,
    WebPubSubStreamingResponse,
)


load_dotenv()

app = WebPubSubInvocationAgentServerHost(
    openapi_spec={
        "openapi": "3.0.0",
        "info": {
            "title": "Web PubSub Streaming Invocation Chat Agent",
            "version": "0.1.0",
        },
        "paths": {
            "/invocations": {
                "post": {
                    "summary": "Start a chat turn and stream response events through Azure Web PubSub.",
                }
            }
        },
    }
)


async def stream_chat(
    message: str,
    conversation_id: str | None,
    context: WebPubSubRunContext,
) -> AsyncIterator[StreamEvent]:
    chunks: list[str] = []
    history = await chat_history.snapshot_with_user_message(conversation_id, message)

    async for delta in stream_chat_completion(history):
        chunks.append(delta)
        yield context.text_delta(delta)

    assistant_message = "".join(chunks)
    await chat_history.commit_turn(conversation_id, message, assistant_message)

    yield context.final_message(assistant_message)


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
        lambda context: stream_chat(message, conversation_id, context),
        user_id=user_id,
        request_id=body.get("request_id"),
        stream_id=body.get("stream_id"),
        conversation_id=conversation_id,
        metadata=body.get("metadata"),
    )


if __name__ == "__main__":
    app.run()
