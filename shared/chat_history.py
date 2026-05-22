from __future__ import annotations

import asyncio
import json
import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Literal, TypedDict


Role = Literal["user", "assistant"]


class ChatMessage(TypedDict):
    role: Role
    content: str


class FileChatHistory:
    def __init__(self, max_messages: int = 20, root: Path | None = None) -> None:
        self._max_messages = max_messages
        self._root = root or Path.home() / ".webpubsub-chat" / "history"
        self._lock = asyncio.Lock()

    async def snapshot_with_user_message(
        self,
        conversation_id: str | None,
        message: str,
    ) -> list[ChatMessage]:
        if not conversation_id:
            return [{"role": "user", "content": message}]

        async with self._lock:
            history = await asyncio.to_thread(self._read_messages, conversation_id)
            snapshot = deepcopy(history)
            snapshot.append({"role": "user", "content": message})
            return snapshot[-self._max_messages :]

    async def commit_turn(
        self,
        conversation_id: str | None,
        user_message: str,
        assistant_message: str,
    ) -> None:
        if not conversation_id:
            return

        async with self._lock:
            messages = await asyncio.to_thread(self._read_messages, conversation_id)
            messages.append({"role": "user", "content": user_message})
            messages.append({"role": "assistant", "content": assistant_message})
            messages = messages[-self._max_messages :]
            await asyncio.to_thread(self._write_messages, conversation_id, messages)

    async def get(self, conversation_id: str) -> list[ChatMessage]:
        async with self._lock:
            return await asyncio.to_thread(self._read_messages, conversation_id)

    async def clear(self, conversation_id: str) -> None:
        async with self._lock:
            path = self._path_for(conversation_id)
            if path.exists():
                await asyncio.to_thread(path.unlink)

    def _read_messages(self, conversation_id: str) -> list[ChatMessage]:
        path = self._path_for(conversation_id)
        if not path.exists():
            return []

        data = json.loads(path.read_text(encoding="utf-8"))
        return [
            {"role": item["role"], "content": item["content"]}
            for item in data
            if item.get("role") in {"user", "assistant"} and isinstance(item.get("content"), str)
        ]

    def _write_messages(self, conversation_id: str, messages: list[ChatMessage]) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        path = self._path_for(conversation_id)
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def _path_for(self, conversation_id: str) -> Path:
        safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", conversation_id)[:160]
        if not safe_id:
            safe_id = "default"
        return self._root / f"{safe_id}.json"


chat_history = FileChatHistory(
    max_messages=int(os.getenv("CHAT_HISTORY_MAX_MESSAGES", "20"))
)
