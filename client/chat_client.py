from __future__ import annotations

import argparse
import json
import os
import threading
import time
import uuid

import requests
from dotenv import load_dotenv
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubclient.models import CallbackType
from azure.messaging.webpubsubservice import WebPubSubServiceClient


def build_client_url(connection_string: str, hub: str, user_id: str) -> str:
    service = WebPubSubServiceClient.from_connection_string(connection_string, hub=hub)
    token = service.get_client_access_token(user_id=user_id)
    return token["url"]


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--agent-url", default="http://localhost:8088/invocations")
    parser.add_argument("--message", required=True)
    parser.add_argument("--user-id", default=f"user-{uuid.uuid4()}")
    parser.add_argument("--conversation-id", default=f"conversation-{uuid.uuid4()}")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    connection_string = os.environ["WEBPUBSUB_CONNECTION_STRING"]
    hub = os.getenv("WEBPUBSUB_HUB", "chat")
    stream_id = f"stream-{uuid.uuid4()}"
    done = threading.Event()

    client = WebPubSubClient(build_client_url(connection_string, hub, args.user_id))

    def on_server_message(event) -> None:
        data = event.data
        if isinstance(data, str):
            data = json.loads(data)

        if data.get("stream_id") != stream_id:
            return

        event_type = data.get("type")
        payload = data.get("data") or {}
        if event_type == "text.delta":
            print(payload.get("text", ""), end="", flush=True)
        elif event_type == "message.final":
            print()
        elif event_type == "error":
            print(f"\nerror: {payload.get('message')}")
            done.set()
        elif event_type == "done":
            done.set()

    client.subscribe(CallbackType.SERVER_MESSAGE, on_server_message)

    with client:
        response = requests.post(
            args.agent_url,
            json={
                "message": args.message,
                "user_id": args.user_id,
                "conversation_id": args.conversation_id,
                "stream_id": stream_id,
            },
            timeout=10,
        )
        print(f"invocation: {response.status_code} {response.text}")

        if not done.wait(args.timeout):
            raise TimeoutError("Timed out waiting for Web PubSub stream completion.")

        time.sleep(0.2)


if __name__ == "__main__":
    main()
