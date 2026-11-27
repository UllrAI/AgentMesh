"""One fresh MQTT 5 client per connection. Queued publishes never cross reconnects."""

import asyncio
import json
import logging
import os
import ssl
import time
from urllib.parse import urlparse

import aiomqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from .config import AGENT_ID

MAX_PAYLOAD = 65536
LOG = logging.getLogger(__name__)


def encode(value):
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()


def decode(data):
    if len(data) > MAX_PAYLOAD:
        raise ValueError("payload exceeds 64 KiB")
    value = json.loads(data.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


class Transport:
    def __init__(self, config, on_packet, on_change):
        self.node = config["agent_id"]
        self.mqtt = config["mqtt"]
        self.on_packet = on_packet
        self.on_change = on_change
        self.card = {"id": self.node, "spec_version": "1.3", "capabilities": []}
        for method in config["runtime"]["methods"]:
            cap = {
                k: method[k]
                for k in ("description", "input_schema", "interaction")
                if k in method
            }
            self.card["capabilities"].append({"method": method["name"], **cap})
        if len(encode(self.card)) > MAX_PAYLOAD:
            raise ValueError("local Card exceeds 64 KiB")
        self.client = None
        self.cards = {}
        self.online_until = {}

    def new_client(self):
        url = urlparse(self.mqtt["url"])
        tls = (
            ssl.create_default_context(cafile=self.mqtt.get("ca_file"))
            if url.scheme == "mqtts"
            else None
        )
        connect = Properties(PacketTypes.CONNECT)
        connect.SessionExpiryInterval = 0
        will = aiomqtt.Will(
            f"agents/{self.node}/status", self.status("offline"), qos=1, retain=True
        )
        return aiomqtt.Client(
            url.hostname,
            url.port or (8883 if tls else 1883),
            identifier=self.node,
            username=self.mqtt["username"],
            password=os.environ[self.mqtt["password_env"]],
            protocol=aiomqtt.ProtocolVersion.V5,
            tls_context=tls,
            will=will,
            clean_start=True,
            keepalive=30,
            properties=connect,
        )

    @staticmethod
    def status(value):
        return encode({"spec_version": "1.3", "status": value})

    async def publish(self, topic, payload, *, expiry, retain=False):
        if len(payload) > MAX_PAYLOAD:
            raise ValueError("payload exceeds 64 KiB")
        client = self.client
        if client is None:
            raise ConnectionError("broker disconnected")
        props = Properties(PacketTypes.PUBLISH)
        if expiry is not None:
            props.MessageExpiryInterval = expiry
        await client.publish(topic, payload, qos=1, retain=retain, properties=props)

    async def heartbeat(self):
        while True:
            await self.publish(
                f"agents/{self.node}/status",
                self.status("online"),
                expiry=180,
                retain=True,
            )
            await asyncio.sleep(60)

    async def run(self):
        while True:
            try:
                async with self.new_client() as client:
                    filters = [
                        (pattern, SubscribeOptions(qos=1))
                        for pattern in ("agents/+/card", "agents/+/status")
                    ]
                    for action in ("invoke", "replies"):
                        filters.append(
                            (
                                f"agents/{self.node}/{action}/+",
                                SubscribeOptions(
                                    qos=1, retainAsPublished=True, retainHandling=2
                                ),
                            )
                        )
                    codes = await client.subscribe(filters)
                    if any(getattr(code, "is_failure", False) for code in codes):
                        raise ConnectionError("broker rejected a subscription")
                    self.cards.clear()
                    self.online_until.clear()
                    self.client = client
                    self.on_change()
                    try:
                        await self.publish(
                            f"agents/{self.node}/card",
                            encode(self.card),
                            expiry=None,
                            retain=True,
                        )
                        async with asyncio.TaskGroup() as group:
                            group.create_task(self.heartbeat())
                            group.create_task(self.receive(client))
                    finally:
                        try:
                            await self.publish(
                                f"agents/{self.node}/status",
                                self.status("offline"),
                                expiry=None,
                                retain=True,
                            )
                        except (aiomqtt.MqttError, OSError):
                            pass
                        self.client = None
                        self.on_change()
            except (ExceptionGroup, aiomqtt.MqttError, OSError) as exc:
                LOG.warning(
                    "broker connection ended (%s), reconnecting", type(exc).__name__
                )
            await asyncio.sleep(1)

    async def receive(self, client):
        async for msg in client.messages:
            parts = str(msg.topic).split("/")
            if (
                len(parts) not in (3, 4)
                or parts[0] != "agents"
                or not AGENT_ID.fullmatch(parts[1])
            ):
                continue
            node, action = parts[1:3]
            try:
                if len(parts) == 3 and action in {"card", "status"}:
                    if not msg.payload:
                        (self.cards if action == "card" else self.online_until).pop(
                            node, None
                        )
                        continue
                    data = decode(msg.payload)
                    if data.get("spec_version") != "1.3":
                        continue
                    if (
                        action == "card"
                        and data.get("id") == node
                        and isinstance(data.get("capabilities"), list)
                    ):
                        self.cards[node] = data
                    if action == "status":
                        ttl = getattr(msg.properties, "MessageExpiryInterval", 0)
                        self.online_until[node] = time.monotonic() + (
                            min(ttl, 180) if data.get("status") == "online" else 0
                        )
                elif (
                    len(parts) == 4
                    and node == self.node
                    and not msg.retain
                    and AGENT_ID.fullmatch(parts[3])
                ):
                    await self.on_packet(
                        action, parts[3], msg.payload, decode(msg.payload)
                    )
            except (ValueError, UnicodeError, RecursionError):
                LOG.debug("discarded malformed %s payload", action)
        raise ConnectionError("MQTT stream closed")

    def list_agents(self, method=None):
        result = []
        for key, card in sorted(self.cards.items()):
            if method and not any(
                isinstance(c, dict) and c.get("method") == method
                for c in card["capabilities"]
            ):
                continue
            status = (
                "unknown"
                if self.client is None
                else (
                    "online"
                    if self.online_until.get(key, 0) > time.monotonic()
                    else "offline"
                )
            )
            result.append({**card, "status": status})
        return result
