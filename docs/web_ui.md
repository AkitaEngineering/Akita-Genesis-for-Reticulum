# Akita Genesis Web UI

Akita Genesis ships with a browser-based control room served directly by each node.

## Access

Start a node with its HTTP API enabled:

```bash
akita-genesis start --node-name ops-node --cluster-name demo --api-port 8001
```

Then open:

```text
http://127.0.0.1:8001/ui
```

## Main Sections

* **Dashboard**: Live node status, uptime, worker availability, task distribution, capabilities, and recent events.
* **Cluster**: Member table with status, role, capability tags, load, and last-seen timing.
* **Tasks**: Task browser with filters plus JSON task submission forms.
* **Logs**: Live log feed with level filtering.
* **Ledger**: Scrollable audit trail with event-type filtering.
* **Configuration**: Sanitized runtime configuration grouped by application, node, network, cluster, task engine, security, and storage.

## Visual System

The control room UI uses a high-contrast operational palette:

* Black and deep charcoal for base surfaces.
* Titanium and silver neutrals for structure and readability.
* Deep purple as the primary action/status accent.
* White/near-white foreground text for critical metrics and controls.

## Security Model

The web UI shell is intentionally public so a browser can load it without custom headers.

Protected behavior:

* `/dashboard/summary`
* `/tasks`
* `/logs`
* `/ledger`
* `/config`
* `/shutdown`

When the node is secured with `AKITA_VALID_API_KEYS`, the UI prompts for an API key and sends it in the configured header for data fetches and control actions.

The key is stored in browser session storage, not persisted indefinitely.

Transport security state (TLS and optional mutual TLS requirements) is exposed in dashboard/config payloads and reflected in UI security context.

## Operational Features

* Manual refresh plus configurable auto-refresh intervals.
* Task submission from both the dashboard and task console.
* Graceful node shutdown action with confirmation.
* Deep links into the OpenAPI docs from the hero panel.

## Backend Endpoints Used

The UI primarily consumes these endpoints:

* `/dashboard/summary`
* `/cluster/status`
* `/tasks`
* `/tasks/submit`
* `/logs`
* `/ledger`
* `/config`
* `/shutdown`
* `/healthz`
* `/readyz`