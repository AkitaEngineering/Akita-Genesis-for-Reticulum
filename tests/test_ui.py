import asyncio

from fastapi.testclient import TestClient
from pydantic import SecretStr

from akita_genesis.config.settings import settings
from akita_genesis.core.node import AkitaGenesisNode
from akita_genesis.modules.state_manager import NodeInfo, NodeStatus


def test_ui_shell_is_public_even_when_api_is_secured(monkeypatch):
    monkeypatch.setattr(settings, "VALID_API_KEYS", {SecretStr("control-room")})

    node = AkitaGenesisNode(run_api_server=False)
    client = TestClient(node.api_app)

    html_response = client.get("/ui")
    assert html_response.status_code == 200
    assert "Akita Genesis Control Room" in html_response.text

    summary_forbidden = client.get("/dashboard/summary")
    assert summary_forbidden.status_code == 403

    summary_response = client.get(
        "/dashboard/summary",
        headers={settings.API_KEY_HEADER_NAME: "control-room"},
    )
    assert summary_response.status_code == 200
    payload = summary_response.json()
    assert payload["security"]["api_secured"] is True
    assert payload["security"]["configured_api_key_count"] == 1


def test_ui_assets_are_served():
    node = AkitaGenesisNode(run_api_server=False)
    client = TestClient(node.api_app)

    response = client.get("/ui/assets/app.js")
    assert response.status_code == 200
    assert "refreshAll" in response.text


def test_cluster_status_counts_online_nodes_correctly():
    node = AkitaGenesisNode(run_api_server=False)
    client = TestClient(node.api_app)

    asyncio.run(
        node.state_manager.local_cluster_state.update_node(
            NodeInfo(
                node_id="node-online",
                node_name="online-worker",
                cluster_name=node.cluster_name,
                status=NodeStatus.ONLINE,
                current_task_count=2,
            )
        )
    )
    asyncio.run(
        node.state_manager.local_cluster_state.update_node(
            NodeInfo(
                node_id="node-offline",
                node_name="offline-worker",
                cluster_name=node.cluster_name,
                status=NodeStatus.OFFLINE,
            )
        )
    )

    response = client.get("/cluster/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["online_nodes_count"] == 1
    assert payload["offline_nodes_count"] == 1
    assert payload["status_counts"]["online"] == 1