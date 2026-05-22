from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

from azure.ai.projects.aio import AIProjectClient
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
from openai import AsyncOpenAI

from .chat_history import ChatMessage


class AsyncChatClient(Protocol):
    chat: object


DEFAULT_SYSTEM_PROMPT = (
    "You are a concise, practical assistant. Answer directly and keep the response "
    "focused on the user's request."
)


@asynccontextmanager
async def create_azure_openai_client() -> AsyncIterator[AsyncChatClient]:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv("FOUNDRY_PROJECT_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")

    if not endpoint:
        raise RuntimeError("AZURE_OPENAI_ENDPOINT or FOUNDRY_PROJECT_ENDPOINT is required.")

    async with _create_foundry_openai_client(endpoint, api_key) as client:
        yield client


def get_deployment_name() -> str:
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv(
        "AZURE_AI_MODEL_DEPLOYMENT_NAME"
    )
    if not deployment:
        raise RuntimeError(
            "AZURE_OPENAI_DEPLOYMENT or AZURE_AI_MODEL_DEPLOYMENT_NAME is required."
        )
    return deployment


@asynccontextmanager
async def _create_foundry_openai_client(
    endpoint: str,
    api_key: str | None,
) -> AsyncIterator[AsyncOpenAI]:
    credential = AsyncDefaultAzureCredential()
    project = AIProjectClient(endpoint=endpoint.rstrip("/"), credential=credential)
    kwargs = {"api_key": api_key} if api_key else {}

    client = project.get_openai_client(**kwargs)
    try:
        yield client
    finally:
        await client.close()
        await project.close()
        await credential.close()


async def stream_chat_completion(messages: list[ChatMessage]) -> AsyncIterator[str]:
    deployment = get_deployment_name()
    system_prompt = os.getenv("CHAT_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)
    request_messages: list[dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        *messages,
    ]

    async with create_azure_openai_client() as client:
        stream = await client.chat.completions.create(
            model=deployment,
            messages=request_messages,
            stream=True,
        )

        async for chunk in stream:
            if not chunk.choices:
                continue

            delta = chunk.choices[0].delta.content
            if delta:
                yield delta
