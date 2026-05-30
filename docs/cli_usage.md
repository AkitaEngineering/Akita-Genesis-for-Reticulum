# Akita Genesis CLI Usage

This document summarizes the current Akita Genesis CLI surface.

## Invocation

If the package is installed, use:

```bash
akita-genesis --help
```

If you are running directly from the repository, use:

```bash
python -m akita_genesis.cli.main --help
```

## Global Options

* `--target-node-api TEXT`: Base URL for the node HTTP API. Default: `http://0.0.0.0:8000`
* `--api-key TEXT`: API key for authenticated requests.
* `--ca-bundle FILE`: Path to a CA bundle for TLS certificate verification.
* `--insecure`: Disable TLS certificate verification (temporary/debug use only).
* `--debug`: Enable debug logging in the CLI.

Environment variables:

* `AKITA_TARGET_NODE_API`
* `AKITA_API_KEY`
* `AKITA_CA_BUNDLE`
* `AKITA_DDNS_EXEC`
* `AKITA_DDNS_CONFIG`
* `AKITA_DDNS_MODULE_PATH`

## Node Lifecycle

Start a node:

```bash
akita-genesis start \
  --node-name worker-a \
  --cluster-name demo-cluster \
  --api-port 8001 \
  --capabilities gpu \
  --capabilities fast_io
```

Useful start options:

* `--identity-path`: Use an explicit Reticulum identity file.
* `--api-host`: Bind the HTTP API to a specific host.
* `--api-port`: Bind the HTTP API to a specific port.
* `--tls-certfile`: Serve the node API over HTTPS with this PEM certificate.
* `--tls-keyfile`: PEM private key for `--tls-certfile`.
* `--tls-ca-file`: Trust store for validating client certs.
* `--tls-require-client-cert`: Enforce mutual TLS (mTLS).
* `--rns-port`: Override the Reticulum transport port.
* `--no-api-server`: Run without the HTTP API.
* `--log-level`: Override the node log level for the current process.

Shut down a node:

```bash
akita-genesis --target-node-api http://127.0.0.1:8001 shutdown
```

## Status Commands

Fetch node status:

```bash
akita-genesis --target-node-api http://127.0.0.1:8001 status
```

Fetch cluster status:

```bash
akita-genesis --target-node-api http://127.0.0.1:8001 cluster status
```

Fetch a concise readiness summary:

```bash
akita-genesis --target-node-api http://127.0.0.1:8001 health
```

## Task Commands

Submit a task:

```bash
akita-genesis task submit '{"action":"resize","required_capabilities":["gpu"]}' --priority 5
```

Get a single task:

```bash
akita-genesis task status <task_id>
```

List tasks:

```bash
akita-genesis task list
akita-genesis task list --status pending --limit 25 --offset 0
```

The task list endpoint now returns actual task records, not just aggregate counts.

## Ledger Commands

View recent ledger entries:

```bash
akita-genesis ledger view --limit 20
akita-genesis ledger view --limit 20 --event-type TASK_COMPLETED
```

## Logs

Fetch recent logs:

```bash
akita-genesis logs --limit 100
akita-genesis logs --limit 50 --level warning
```

Follow logs continuously by polling the node API:

```bash
akita-genesis logs --follow
akita-genesis logs --follow --level error
```

## Security Notes

If API keys are configured on the node, pass one with `--api-key` or set `AKITA_API_KEY`.

On the node side, `AKITA_VALID_API_KEYS` accepts either a comma-separated string:

```bash
export AKITA_VALID_API_KEYS='key1,key2,key3'
```

Or a JSON array:

```bash
export AKITA_VALID_API_KEYS='["key1", "key2", "key3"]'
```

Use HTTPS and optional mTLS for mission-critical control-plane deployments:

```bash
akita-genesis start --api-port 8443 --tls-certfile ./server.crt --tls-keyfile ./server.key
akita-genesis start --api-port 8443 --tls-certfile ./server.crt --tls-keyfile ./server.key --tls-ca-file ./clients-ca.pem --tls-require-client-cert

akita-genesis --target-node-api https://127.0.0.1:8443 --ca-bundle ./ca.pem status
```

Global CLI examples are available directly:

```bash
akita-genesis examples --topic all
akita-genesis examples --topic security
```

For UI operations:

```bash
akita-genesis ui url
akita-genesis ui url --open-browser
akita-genesis dashboard
```

## Optional Akita DDNS Integration

Akita Genesis includes an optional DDNS bridge for the external Akita Dynamic DDNS project.

Quick checks:

```bash
akita-genesis ddns doctor --config ./akita_config.yaml
akita-genesis ddns list --config ./akita_config.yaml
```

Register/resolve names:

```bash
akita-genesis ddns register --name alpha.prod --ttl 3600 --config ./akita_config.yaml
akita-genesis ddns resolve --name alpha.prod --config ./akita_config.yaml
```

Register the current node from Akita Genesis status:

```bash
akita-genesis --target-node-api http://127.0.0.1:8001 ddns register-node --config ./akita_config.yaml
```

If `akita_ddns` is not installed into the same environment, provide the source path:

```bash
akita-genesis ddns doctor --module-path /path/to/Akita-Dynamic-DDNS-for-Reticulum --config /path/to/akita_config.yaml
```

## Browser Control Room

The node also serves a browser UI at `/ui`.

Example:

```bash
python -m akita_genesis.cli.main start --node-name ops-node --cluster-name demo --api-port 8001
# open http://127.0.0.1:8001/ui
```

The browser UI uses the same node API underneath. If the node is secured, open the API key dialog in the UI and provide a valid key.