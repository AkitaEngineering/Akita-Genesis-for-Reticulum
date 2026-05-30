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
* `--debug`: Enable debug logging in the CLI.

Environment variables:

* `AKITA_TARGET_NODE_API`
* `AKITA_API_KEY`

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
akita-genesis task list --status pending --limit 25
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

## Browser Control Room

The node also serves a browser UI at `/ui`.

Example:

```bash
python -m akita_genesis.cli.main start --node-name ops-node --cluster-name demo --api-port 8001
# open http://127.0.0.1:8001/ui
```

The browser UI uses the same node API underneath. If the node is secured, open the API key dialog in the UI and provide a valid key.