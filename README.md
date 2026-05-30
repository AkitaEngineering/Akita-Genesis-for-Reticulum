# Akita Genesis

Akita Genesis is a distributed-systems framework built around Reticulum. It provides node discovery, cluster state tracking, task scheduling, resource monitoring, a persistent event ledger, and a CLI/API control surface for operating nodes.

License: GPLv3

## What It Does

* Discovers nodes over Reticulum and groups them into named clusters.
* Elects a leader and maintains cluster state, node health, capabilities, and task load.
* Accepts tasks through the HTTP API or CLI and schedules them by priority, capability, and worker load.
* Tracks task retries, worker acknowledgement timeouts, and processing timeouts.
* Exposes node status, cluster status, task state, logs, and ledger events through a CLI-friendly HTTP API.
* Serves a browser-based control room with dashboards, cluster views, task browsing/submission, logs, ledger history, and grouped configuration panes.
* Supports optional API key protection for the control API.
* Supports HTTPS for the control API, with optional mutual TLS for operator-grade deployments.
* Exposes Linux-friendly liveness/readiness probes at `/healthz` and `/readyz`.
* Optionally integrates with Akita Dynamic DDNS for human-readable Reticulum naming.

## Requirements

* Python 3.8+
* A virtual environment is strongly recommended.
* Dependencies listed in `requirements.txt`

## Quick Start

1. Clone the repository.

```bash
git clone https://github.com/AkitaEngineering/Akita-Genesis-for-Reticulum
cd Akita-Genesis-for-Reticulum
```

2. Create and activate a virtual environment.

```bash
python -m venv .venv
source .venv/bin/activate
```

3. Install runtime and development dependencies.

```bash
pip install -r requirements.txt -r dev-requirements.txt
pip install -e .
```

4. Confirm the CLI is available.

```bash
akita-genesis --help
```

If you do not want to install the console script yet, you can run the CLI as a module:

```bash
python -m akita_genesis.cli.main --help
```

## Configuration

Settings are loaded from environment variables prefixed with `AKITA_` and from an optional `.env` file in the repository root.

Common settings:

* `AKITA_LOG_LEVEL`
* `AKITA_SQLITE_DB_FILE`
* `AKITA_DATA_DIR`
* `AKITA_DEFAULT_CLUSTER_NAME`
* `AKITA_DEFAULT_API_HOST`
* `AKITA_DEFAULT_API_PORT`
* `AKITA_API_TLS_CERT_FILE`
* `AKITA_API_TLS_KEY_FILE`
* `AKITA_API_TLS_CA_FILE`
* `AKITA_API_TLS_REQUIRE_CLIENT_CERT`
* `AKITA_MAX_TASK_EXECUTION_ATTEMPTS`
* `AKITA_WORKER_ACK_TIMEOUT_S`
* `AKITA_WORKER_PROCESSING_TIMEOUT_S`

API keys can now be provided in either of these formats:

```bash
export AKITA_VALID_API_KEYS='key1,key2,key3'
export AKITA_VALID_API_KEYS='["key1", "key2", "key3"]'
```

If `AKITA_VALID_API_KEYS` is empty or unset, the HTTP control API is unsecured.

## CLI Overview

Global options:

* `--target-node-api`: Base URL for the node HTTP API.
* `--api-key`: API key sent in the configured header.
* `--ca-bundle`: CA bundle path for verifying HTTPS node certificates.
* `--insecure`: Disable HTTPS certificate verification for temporary bring-up.
* `--debug`: Enables debug logging for the CLI.

Common commands:

```bash
# Start a node
akita-genesis start --node-name WorkerGPU --cluster-name MyCluster --capabilities gpu --api-port 8001

# Start HTTPS with optional mutual TLS
akita-genesis start --api-port 8443 --tls-certfile ./server.crt --tls-keyfile ./server.key
akita-genesis start --api-port 8443 --tls-certfile ./server.crt --tls-keyfile ./server.key --tls-ca-file ./clients-ca.pem --tls-require-client-cert

# Check node and cluster status
akita-genesis --target-node-api http://127.0.0.1:8001 status
akita-genesis --target-node-api http://127.0.0.1:8001 cluster status
akita-genesis --target-node-api https://127.0.0.1:8443 --ca-bundle ./ca.pem status

# Submit and inspect tasks
akita-genesis task submit '{"action":"train","required_capabilities":["gpu"]}' --priority 5
akita-genesis task list --status pending --limit 20
akita-genesis task status <task_id>

# View logs once or follow them continuously
akita-genesis logs --limit 50
akita-genesis logs --follow --level error

# Inspect the ledger
akita-genesis ledger view --limit 20 --event-type TASK_COMPLETED

# Runtime probes and high-level control room links
akita-genesis --target-node-api http://127.0.0.1:8001 health
akita-genesis --target-node-api http://127.0.0.1:8001 ui url --open-browser

# Curated operator examples
akita-genesis examples --topic operations

# Optional Akita DDNS integration
akita-genesis ddns doctor
akita-genesis --target-node-api http://127.0.0.1:8001 ddns register-node
akita-genesis ddns resolve --name workergpu.mycluster
```

## Akita DDNS Integration (Optional)

The Akita DDNS project is valuable for Reticulum-native service discovery because it maps human-readable names to dynamic RIDs.

This repository now includes optional CLI integration through the `ddns` command group, including:

* `ddns doctor` to verify DDNS availability
* `ddns register` and `ddns resolve`
* `ddns register-node` to publish the current Akita node RID from `/status`
* `ddns list` to inspect persisted DDNS state

Important:

* Akita DDNS is not added as a hard runtime dependency because the external repository currently does not publish package metadata (`setup.py` or `pyproject.toml`).
* Integration is process-based (`python -m akita_ddns.main ...`) and remains optional.

To use it reliably from this project, point the CLI to the DDNS module path:

```bash
export AKITA_DDNS_MODULE_PATH=/path/to/Akita-Dynamic-DDNS-for-Reticulum
akita-genesis ddns doctor --config /path/to/akita_config.yaml
```

For a fuller command reference, see `docs/cli_usage.md`.

## Browser UI

Each node now serves a browser control room at `/ui`.

Example:

```bash
akita-genesis start --node-name WorkerGPU --cluster-name MyCluster --api-port 8001
# then open http://127.0.0.1:8001/ui
```

The UI includes:

* A live dashboard with node health, cluster posture, task distribution, and recent events.
* Dedicated sections for cluster members, tasks, logs, ledger history, and runtime configuration.
* Browser-side task submission and a graceful shutdown action.
* Auto-refresh controls and an API key dialog for secured nodes.

Security model:

* The UI shell itself is public so a browser can load it normally.
* Data endpoints and control actions still require the API key when the node is secured.
* The browser stores the provided key in session storage for the current tab/session.
* The UI now defaults to a black/silver/titanium/deep-purple control-room palette for high-contrast operations.

## Linux Operations Notes

For service-managed Linux environments (systemd, containers, orchestrators), use:

* `/healthz` for liveness checks.
* `/readyz` for readiness checks and startup gates.

When TLS is enabled on the node API:

* Prefer `--ca-bundle` on the CLI.
* Keep `--insecure` only for temporary debugging.

## Development

Run tests:

```bash
python -m pytest -q
```

Run type checks:

```bash
python -m mypy akita_genesis tests
```

Optional style tools:

```bash
python -m flake8 akita_genesis tests
python -m black --check .
python -m isort --check-only .
```

## Documentation

* `docs/architecture.md` describes the main runtime components and message flow.
* `docs/cli_usage.md` documents the CLI commands and examples.
* `docs/web_ui.md` documents the browser control room and its data flows.

## Contributing

Contributions should keep behavior covered by tests and update documentation when CLI or API behavior changes.

