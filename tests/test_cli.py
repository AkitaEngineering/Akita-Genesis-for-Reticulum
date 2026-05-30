from click.testing import CliRunner

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