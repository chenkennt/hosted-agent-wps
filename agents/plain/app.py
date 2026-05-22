from __future__ import annotations

import json
from typing import Any, AsyncIterator

from azure.ai.agentserver.invocations import InvocationAgentServerHost
from dotenv import load_dotenv
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse

from shared.azure_openai_chat import stream_chat_completion
from shared.chat_history import chat_history


load_dotenv()

app = InvocationAgentServerHost(
    openapi_spec={
        "openapi": "3.0.0",
        "info": {
            "title": "Plain Streaming Invocation Chat Agent",
            "version": "0.1.0",
        },
        "paths": {
            "/invocations": {
                "post": {
                    "summary": "Start a chat turn and stream response events directly over SSE.",
                }
            }
        },
    }
)


def sse_event(event_type: str, data: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"


async def stream_chat(message: str, conversation_id: str | None) -> AsyncIterator[str]:
    chunks: list[str] = []
    sequence = 0

    try:
        history = await chat_history.snapshot_with_user_message(conversation_id, message)
        async for delta in stream_chat_completion(history):
            sequence += 1
            chunks.append(delta)
            yield sse_event(
                "text.delta",
                {
                    "sequence": sequence,
                    "data": {
                        "text": delta,
                    },
                },
            )
    except Exception as exc:
        yield sse_event(
            "error",
            {
                "sequence": sequence + 1,
                "data": {
                    "message": str(exc),
                },
            },
        )
        return

    assistant_message = "".join(chunks)
    await chat_history.commit_turn(conversation_id, message, assistant_message)

    yield sse_event(
        "message.final",
        {
            "sequence": sequence + 1,
            "data": {
                "role": "assistant",
                "content": assistant_message,
            },
        },
    )
    yield sse_event("done", {"data": {}})


@app.invoke_handler
async def invoke(request: Request):
    body = await request.json()
    message = str(body.get("message") or "").strip()
    conversation_id = body.get("conversation_id")
    if not message:
        return JSONResponse(
            {"status": "rejected", "error": "Request body must include message."},
            status_code=400,
        )

    return StreamingResponse(
        stream_chat(message, conversation_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    app.run()
