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


def test_health_and_ready_routes_are_public():
    node = AkitaGenesisNode(run_api_server=False)
    client = TestClient(node.api_app)

    health_response = client.get("/healthz")
    ready_response = client.get("/readyz")

    assert health_response.status_code == 200
    assert ready_response.status_code == 200
    assert health_response.json()["status"] == "alive"
    assert ready_response.json()["ready"] is True
    assert ready_response.json()["api_scheme"] == "http"


def test_tls_configuration_is_reflected_in_security_snapshots():
    node = AkitaGenesisNode(
        run_api_server=False,
        api_tls_certfile="/tmp/server.crt",
        api_tls_keyfile="/tmp/server.key",
        api_tls_ca_file="/tmp/clients.pem",
        api_tls_require_client_cert=True,
    )

    config_snapshot = node._build_config_snapshot()
    dashboard_summary = asyncio.run(node._build_dashboard_summary())

    assert node.api_scheme == "https"
    assert config_snapshot["security"]["tls_enabled"] is True
    assert config_snapshot["security"]["mutual_tls_required"] is True
    assert dashboard_summary["security"]["tls_enabled"] is True
    assert dashboard_summary["security"]["mutual_tls_required"] is True


def test_tls_configuration_requires_cert_and_key_together():
    try:
        AkitaGenesisNode(run_api_server=False, api_tls_certfile="/tmp/server.crt")
    except ValueError as exc:
        assert "certificate file and a private key file" in str(exc)
    else:
        raise AssertionError("Expected TLS configuration validation to fail")