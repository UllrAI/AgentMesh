"""Create a private, loopback-only two-node demo in a new directory."""

import argparse
import secrets
import shlex
import shutil
import subprocess
from pathlib import Path

import yaml


def setup(directory: Path, port: int):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    acl = directory / "acl"
    shutil.copyfile(Path(__file__).with_name("acl"), acl)
    password_file = directory / "passwords"
    secrets_file = directory / "env"
    with secrets_file.open("x") as env:
        for index, node in enumerate(("caller", "worker")):
            secret = secrets.token_urlsafe(32)
            variable = "MESH_" + node.upper() + "_PASSWORD"
            env.write(f"export {variable}={shlex.quote(secret)}\n")
            command = ["mosquitto_passwd", "-b"]
            if index == 0:
                command.append("-c")
            subprocess.run([*command, str(password_file), node, secret], check=True)
            config = {
                "agent_id": node,
                "mqtt": {
                    "url": f"mqtt://127.0.0.1:{port}",
                    "username": node,
                    "password_env": variable,
                },
                "socket": str(directory / (node + ".sock")),
                "job_store": str(directory / (node + ".db")),
                "runtime": {
                    "type": "echo",
                    "workdir": str(directory / "project"),
                    "methods": [],
                },
            }
            if node == "worker":
                config["runtime"]["methods"].append(
                    {
                        "name": "demo.analyze",
                        "description": "Analyze the demo project with an explicit API or Web scope",
                        "interaction": "clarify",
                        "input_schema": {
                            "type": "object",
                            "required": ["prompt"],
                            "properties": {
                                "prompt": {"type": "string"},
                                "scope": {"type": "string"},
                            },
                        },
                    }
                )
            (directory / f"{node}.yaml").write_text(
                yaml.safe_dump(config, sort_keys=False)
            )
    secrets_file.chmod(0o600)
    (directory / "project").mkdir()
    lines = [
        f"listener {port} 127.0.0.1",
        "allow_anonymous false",
        "use_username_as_clientid true",
        f"password_file {password_file}",
        f"acl_file {acl}",
        "persistence true",
        f"persistence_location {directory}/",
    ]
    (directory / "mosquitto.conf").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--port", type=int, default=18883)
    args = parser.parse_args()
    setup(args.directory.resolve(), args.port)
