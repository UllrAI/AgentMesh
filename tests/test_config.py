import pytest
import yaml

from agent_mesh.config import load_config, socket_path


def test_local_config_needs_id_and_env_only_for_daemon(tmp_path, monkeypatch):
    path = tmp_path / "node.yaml"
    config = {
        "agent_id": "worker",
        "socket": str(tmp_path / "agent.sock"),
        "mqtt": {
            "url": "mqtt://127.0.0.1:1883",
            "username": "worker",
            "password_env": "TEST_NODE_CREDENTIAL",
        },
    }
    path.write_text(yaml.safe_dump(config))
    monkeypatch.delenv("AGENT_MESH_SOCKET", raising=False)
    monkeypatch.delenv("TEST_NODE_CREDENTIAL", raising=False)
    assert socket_path(None, path) == config["socket"]
    with pytest.raises(ValueError, match="password_env"):
        load_config(path)
    monkeypatch.setenv("TEST_NODE_CREDENTIAL", "local-test")
    assert load_config(path)["runtime"]["max_concurrent_jobs"] == 1
    config["mqtt"]["username"] = "other"
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match="match agent_id"):
        load_config(path)


@pytest.mark.parametrize("content", ["", "[", "[]"])
def test_invalid_yaml_has_readable_error(tmp_path, content):
    path = tmp_path / "invalid"
    path.write_text(content)
    with pytest.raises(ValueError):
        load_config(path)
