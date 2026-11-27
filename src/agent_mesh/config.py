"""Read the node's local configuration. Remote calls cannot override these settings."""

import os
import re
from pathlib import Path
from urllib.parse import urlparse

import jsonschema
import yaml

AGENT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
DEFAULT_SOCKET = "~/.agent-mesh/agent.sock"


def _read_mapping(path):
    try:
        with Path(path).expanduser().open() as stream:
            config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ValueError("invalid YAML configuration") from exc
    if not isinstance(config, dict):
        raise ValueError("configuration must be a YAML mapping")
    return config


def load_config(path):
    config = _read_mapping(path)
    if not isinstance(config.get("agent_id"), str) or not AGENT_ID.fullmatch(
        config["agent_id"]
    ):
        raise ValueError("expected a valid agent_id")
    mqtt = config.get("mqtt")
    if not isinstance(mqtt, dict):
        raise ValueError("mqtt configuration required")
    if not isinstance(mqtt.get("url"), str):
        raise ValueError("mqtt.url must be a string")
    url = urlparse(mqtt["url"])
    if (
        url.scheme not in {"mqtt", "mqtts"}
        or not url.hostname
        or url.path not in ("", "/")
        or url.params
        or url.query
        or url.fragment
        or url.username
        or url.password
    ):
        raise ValueError(
            "mqtt.url must be mqtt://host or mqtts://host; use password_env for credentials"
        )
    if url.port == 0:
        raise ValueError("invalid broker port")
    if mqtt.get("username") != config["agent_id"]:
        raise ValueError("mqtt.username must match agent_id")
    env = mqtt.get("password_env")
    if not isinstance(env, str) or not os.environ.get(env):
        raise ValueError("mqtt.password_env must name a populated environment variable")
    node = config["agent_id"]
    config["socket"] = os.path.abspath(
        os.path.expanduser(config.get("socket", DEFAULT_SOCKET))
    )
    config["job_store"] = os.path.abspath(
        os.path.expanduser(config.get("job_store", f"~/.agent-mesh/{node}.db"))
    )
    runtime = config.setdefault("runtime", {})
    if not isinstance(runtime, dict):
        raise ValueError("runtime must be a mapping")
    for key, default in (("max_concurrent_jobs", 1), ("execution_timeout_s", 300)):
        n = runtime.setdefault(key, default)
        if type(n) is not int or n <= 0:
            raise ValueError(f"runtime.{key} must be a positive integer")
    runtime["workdir"] = os.path.abspath(
        os.path.expanduser(runtime.get("workdir", "."))
    )
    methods = runtime.setdefault("methods", [])
    names = set()
    if not isinstance(methods, list):
        raise ValueError("runtime.methods must be a list")
    for method in methods:
        if not isinstance(method, dict):
            raise ValueError("runtime method must be a mapping")
        name = method.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("each method needs a unique name")
        names.add(name)
        if method.setdefault("interaction", "single_turn") not in {
            "single_turn",
            "session",
            "clarify",
        }:
            raise ValueError(f"unsupported interaction for {name}")
        if "input_schema" in method:
            jsonschema.validators.validator_for(method["input_schema"]).check_schema(
                method["input_schema"]
            )
    return config


def socket_path(socket_arg=None, config_path=None):
    path = socket_arg or os.environ.get("AGENT_MESH_SOCKET")
    if not path and config_path:
        # Clients only need the socket path, not the daemon's broker credentials.
        path = _read_mapping(config_path).get("socket")
    return os.path.abspath(os.path.expanduser(path or DEFAULT_SOCKET))
