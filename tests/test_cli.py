from click.testing import CliRunner
import subprocess

import akita_genesis.cli.main as cli_main


def test_logs_follow_builds_endpoint_before_polling(monkeypatch):
    called_endpoints = []

    def fake_make_api_request(ctx, method, endpoint, **kwargs):
        called_endpoints.append((method, endpoint))
        return {
            "node_id": "node-1",
            "logs": [
                {
                    "timestamp": 1.0,
                    "level": "INFO",
                    "logger": "akita.tests",
                    "message": "hello",
                }
            ],
        }

    def stop_after_first_poll(_seconds):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli_main, "make_api_request", fake_make_api_request)
    monkeypatch.setattr(cli_main.time, "sleep", stop_after_first_poll)

    runner = CliRunner()
    result = runner.invoke(
        cli_main.cli_app,
        [
            "--target-node-api",
            "http://example.test",
            "logs",
            "--follow",
            "--limit",
            "1",
            "--level",
            "error",
        ],
    )

    assert result.exit_code == 0
    assert called_endpoints == [("GET", "/logs?limit=1&level=ERROR")]
    assert "Following logs" in result.output
    assert "Stopped following logs." in result.output


def test_examples_command_renders_security_examples():
    runner = CliRunner()
    result = runner.invoke(cli_main.cli_app, ["examples", "--topic", "security"])

    assert result.exit_code == 0
    assert "Security Examples" in result.output
    assert "--ca-bundle ./ca.pem" in result.output
    assert "https://node.example:8443" in result.output


def test_ui_url_command_prints_control_room_url():
    runner = CliRunner()
    result = runner.invoke(
        cli_main.cli_app,
        ["--target-node-api", "https://node.example:8443", "ui", "url"],
    )

    assert result.exit_code == 0
    assert "https://node.example:8443/ui" in result.output


def test_task_list_includes_offset_parameter(monkeypatch):
    called_endpoints = []

    def fake_make_api_request(ctx, method, endpoint, **kwargs):
        called_endpoints.append((method, endpoint))
        return {"tasks": [], "count": 0}

    monkeypatch.setattr(cli_main, "make_api_request", fake_make_api_request)

    runner = CliRunner()
    result = runner.invoke(
        cli_main.cli_app,
        [
            "--target-node-api",
            "http://example.test",
            "task",
            "list",
            "--status",
            "pending",
            "--limit",
            "25",
            "--offset",
            "50",
        ],
    )

    assert result.exit_code == 0
    assert called_endpoints == [("GET", "/tasks?limit=25&offset=50&status=pending")]


def test_ddns_register_node_uses_node_status_and_executes_ddns_cli(monkeypatch):
    ddns_commands = []

    def fake_make_api_request(ctx, method, endpoint, **kwargs):
        assert method == "GET"
        assert endpoint == "/status"
        return {
            "node_name": "Alpha Worker",
            "cluster_name": "Prod Cluster",
            "rns_identity_hex": "abcd1234",
        }

    def fake_run(command, check, text, capture_output, timeout, env):
        ddns_commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="Registration sent", stderr="")

    monkeypatch.setattr(cli_main, "make_api_request", fake_make_api_request)
    monkeypatch.setattr(cli_main.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        cli_main.cli_app,
        [
            "--target-node-api",
            "http://example.test",
            "ddns",
            "register-node",
            "--config",
            "./akita_config.yaml",
        ],
    )

    assert result.exit_code == 0
    assert len(ddns_commands) == 1
    command = ddns_commands[0]
    assert command[:3] == ["python", "-m", "akita_ddns.main"]
    assert "--config" in command
    assert "./akita_config.yaml" in command
    assert "register" in command
    assert "--name" in command
    assert "alpha-worker.prod-cluster" in command
    assert "--rid" in command
    assert "abcd1234" in command


def test_ddns_doctor_runs_cli_help(monkeypatch):
    ddns_commands = []

    def fake_run(command, check, text, capture_output, timeout, env):
        ddns_commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="usage", stderr="")

    monkeypatch.setattr(cli_main.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        cli_main.cli_app,
        [
            "ddns",
            "doctor",
            "--config",
            "./akita_config.yaml",
        ],
    )

    assert result.exit_code == 0
    assert len(ddns_commands) == 1
    command = ddns_commands[0]
    assert command[:3] == ["python", "-m", "akita_ddns.main"]
    assert command[-2:] == ["cli", "--help"]