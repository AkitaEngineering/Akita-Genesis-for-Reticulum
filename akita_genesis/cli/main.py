# akita_genesis/cli/main.py
import asyncio
import json
import logging
import textwrap
import click
import requests 
import os
import shlex
import subprocess
import sys
import time 
import webbrowser
import re # For log parsing
from typing import Any, Dict, List, Optional, Tuple

# Rich for pretty CLI output
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.syntax import Syntax
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

# Ensure the project root is in the Python path for direct execution from project root
# This is mainly for development. When installed as a package, imports should work directly.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)



def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


CLI_DEFAULT_API_HOST = os.environ.get("AKITA_DEFAULT_API_HOST", "0.0.0.0")
CLI_DEFAULT_API_PORT = _env_int("AKITA_DEFAULT_API_PORT", 8000)
CLI_DEFAULT_CLUSTER_NAME = os.environ.get("AKITA_DEFAULT_CLUSTER_NAME", "default_cluster")
CLI_DEFAULT_LOG_LEVEL = os.environ.get("AKITA_LOG_LEVEL", "INFO").upper()
CLI_DEFAULT_TASK_PRIORITY = _env_int("AKITA_DEFAULT_TASK_PRIORITY", 10)
CLI_DEFAULT_CONTROL_API_TIMEOUT_S = _env_int("AKITA_CONTROL_API_TIMEOUT_S", 10)
CLI_DEFAULT_API_KEY_HEADER_NAME = os.environ.get("AKITA_API_KEY_HEADER_NAME", "X-API-Key")
CLI_DEFAULT_DDNS_EXEC = os.environ.get("AKITA_DDNS_EXEC", "python -m akita_ddns.main")
CLI_DEFAULT_DDNS_CONFIG = os.environ.get("AKITA_DDNS_CONFIG", "akita_config.yaml")
CLI_DEFAULT_DDNS_MODULE_PATH = os.environ.get("AKITA_DDNS_MODULE_PATH", "")
CONFIG_SECTION_CHOICES = [
    "application",
    "node",
    "network",
    "cluster",
    "task_engine",
    "security",
    "storage",
]
CLI_HELP_EPILOG = textwrap.dedent(
    """
    Examples:
      akita-genesis start --node-name alpha --cluster-name prod --api-port 8001
      akita-genesis --target-node-api http://127.0.0.1:8001 dashboard
      akita-genesis --target-node-api http://127.0.0.1:8001 task submit '{"action":"train"}' --priority 5
      akita-genesis --target-node-api https://node.example:8443 --ca-bundle ./ca.pem status
            akita-genesis --target-node-api http://127.0.0.1:8001 ddns register-node
      akita-genesis examples --topic security
    """
).strip()
EXAMPLE_CATALOG: Dict[str, List[Tuple[str, str]]] = {
    "bootstrap": [
        (
            "Start a node with explicit cluster membership and capabilities",
            "akita-genesis start --node-name alpha --cluster-name prod --api-port 8001 --capabilities gpu --capabilities fast_io",
        ),
        (
            "Start a node without the HTTP API for transport-only testing",
            "akita-genesis start --node-name transport-only --no-api-server --cluster-name lab",
        ),
    ],
    "operations": [
        (
            "Show the high-level dashboard summary for a node",
            "akita-genesis --target-node-api http://127.0.0.1:8001 dashboard",
        ),
        (
            "Inspect the cluster state with pretty tabular output",
            "akita-genesis --target-node-api http://127.0.0.1:8001 cluster status",
        ),
        (
            "Print the browser control room URL and open it",
            "akita-genesis --target-node-api http://127.0.0.1:8001 ui url --open-browser",
        ),
    ],
    "tasks": [
        (
            "Submit a capability-aware task",
            "akita-genesis task submit '{\"action\":\"train\",\"required_capabilities\":[\"gpu\"]}' --priority 5",
        ),
        (
            "List recent assigned tasks with pagination",
            "akita-genesis task list --status assigned --limit 20 --offset 20",
        ),
        (
            "Fetch a single task in machine-readable JSON",
            "akita-genesis task status <task-id>",
        ),
    ],
    "security": [
        (
            "Use a custom CA bundle for TLS verification",
            "akita-genesis --target-node-api https://node.example:8443 --ca-bundle ./ca.pem status",
        ),
        (
            "Temporarily bypass TLS certificate verification during bring-up",
            "akita-genesis --target-node-api https://node.example:8443 --insecure logs --limit 20",
        ),
        (
            "Authenticate to a secured node API",
            "akita-genesis --target-node-api https://node.example:8443 --api-key <token> dashboard",
        ),
    ],
    "ddns": [
        (
            "Auto-register the current Akita node identity in Akita DDNS",
            "akita-genesis --target-node-api http://127.0.0.1:8001 ddns register-node",
        ),
        (
            "Resolve a DDNS name via Akita DDNS CLI integration",
            "akita-genesis ddns resolve --name alpha.prod --config ./akita_config.yaml",
        ),
        (
            "Check whether Akita DDNS CLI is available in this environment",
            "akita-genesis ddns doctor --config ./akita_config.yaml",
        ),
    ],
}
_cached_settings: Optional[Any] = None


def setup_cli_logger(name: str = "akita_cli", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - [%(levelname)s] - %(name)s (%(filename)s.%(funcName)s:%(lineno)d) - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_settings() -> Any:
    global _cached_settings
    if _cached_settings is None:
        from akita_genesis.config.settings import settings as app_settings

        _cached_settings = app_settings
    return _cached_settings


def get_api_key_header_name() -> str:
    try:
        return str(get_settings().API_KEY_HEADER_NAME)
    except Exception:
        return CLI_DEFAULT_API_KEY_HEADER_NAME


def get_control_api_timeout() -> int:
    try:
        return int(get_settings().CONTROL_API_TIMEOUT_S)
    except Exception:
        return CLI_DEFAULT_CONTROL_API_TIMEOUT_S


def normalize_status(value: Any) -> str:
    if value is None:
        return "unknown"
    return str(value).lower()


def status_style(status: str) -> str:
    normalized = normalize_status(status)
    if normalized == "online":
        return "bold green"
    if normalized == "degraded":
        return "bold yellow"
    if normalized in {"offline", "failed", "error"}:
        return "bold red"
    return "bold white"


def build_ui_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/ui"


def normalize_ddns_name(raw_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9.-]+", "-", raw_name.strip().lower())
    normalized = re.sub(r"-+", "-", normalized)
    return normalized.strip(".-")


def _run_ddns_subprocess(
    ddns_exec: str,
    ddns_config: str,
    mode: str,
    args: List[str],
    module_path: str = "",
    timeout_s: int = 15,
) -> subprocess.CompletedProcess:
    cmd = shlex.split(ddns_exec)
    cmd.extend(["--config", ddns_config, mode])
    cmd.extend(args)
    cli_log.debug(f"Executing DDNS command: {' '.join(cmd)}")
    environment = os.environ.copy()
    if module_path:
        existing_pythonpath = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = f"{module_path}:{existing_pythonpath}" if existing_pythonpath else module_path
    return subprocess.run(cmd, check=False, text=True, capture_output=True, timeout=timeout_s, env=environment)


def _print_subprocess_result(result: subprocess.CompletedProcess) -> None:
    if result.stdout:
        click.echo(result.stdout.strip())
    if result.stderr:
        click.echo(result.stderr.strip(), err=True)


def render_examples(topic: str) -> None:
    console = Console(theme=log_theme)
    topics = list(EXAMPLE_CATALOG.keys()) if topic == "all" else [topic]
    for topic_name in topics:
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Use Case", ratio=1)
        table.add_column("Command", ratio=2)
        for description, command in EXAMPLE_CATALOG[topic_name]:
            table.add_row(description, Syntax(command, "bash", theme="native", word_wrap=True))
        console.print(Panel(table, title=f"{topic_name.title()} Examples", border_style="blue"))

# --- Rich Highlighter for Logs ---
class LogHighlighter(RegexHighlighter):
    """Apply style rules to log text."""
    base_style = "log."
    highlights = [
        r"(?P<timestamp>^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", # Timestamp
        r"(?P<level>\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\])", # Log Level
        r"(?P<node_id>\b[0-9a-fA-F]{12}\b)", # Node ID (12 hex chars)
        r"(?P<task_id>\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b)", # UUID Task ID
        r"(?P<path>\([\w\.]+\.py:\d+\))", # Python path like (module.py:123)
        r"(?P<number>\b\d+\.?\d*\b)", # Numbers
        r"(?P<boolean>True|False|None)", # Booleans/None
        r"'(?P<string>.*?)'", # Single quoted strings
    ]

log_theme = Theme({
    "log.timestamp": "green",
    "log.level": "bold", # Default style, specific levels below
    "log.level.DEBUG": "dim cyan",
    "log.level.INFO": "blue",
    "log.level.WARNING": "yellow",
    "log.level.ERROR": "bold red",
    "log.level.CRITICAL": "bold white on red",
    "log.node_id": "bold magenta",
    "log.task_id": "bold blue",
    "log.path": "dim italic",
    "log.number": "magenta",
    "log.boolean": "bold cyan",
    "log.string": "bright_black",
})

# --- CLI Logger ---
cli_log = setup_cli_logger("akita_cli", level="INFO")

# --- API Request Helper (UPDATED for API Key) ---
def make_api_request(ctx, method: str, endpoint: str, **kwargs) -> dict:
    """
    Helper function to make requests to the node's API.
    Uses target_node_api and api_key from click context.
    """
    base_url = ctx.obj.get('TARGET_NODE_API')
    api_key = ctx.obj.get('API_KEY') # Get API key from context
    ca_bundle = ctx.obj.get('CA_BUNDLE')
    insecure = ctx.obj.get('INSECURE', False)
    api_key_header_name = get_api_key_header_name()
    api_timeout = get_control_api_timeout()
    api_secured_locally = False
    try:
        api_secured_locally = bool(get_settings().VALID_API_KEYS)
    except Exception:
        api_secured_locally = False
    if not base_url:
        cli_log.error("Target node API URL is not set. Use --target-node-api <URL>.")
        raise click.Abort()

    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    cli_log.debug(f"Making API {method} request to: {url}")
    
    headers = kwargs.pop('headers', {}) # Get existing headers or create new dict
    if api_key:
        headers[api_key_header_name] = api_key
        cli_log.debug(f"Using API Key ending in '...{api_key[-4:] if len(api_key) >= 4 else api_key}'")
    elif api_secured_locally: # Warn if keys are configured but none provided
         cli_log.warning(f"No API key provided (--api-key or AKITA_API_KEY). Request to {url} might fail if API is secured.")

    verify: Any = kwargs.pop('verify', None)
    if verify is None:
        verify = False if insecure else (ca_bundle if ca_bundle else True)
         
    try:
        response = requests.request(method, url, headers=headers, timeout=api_timeout, verify=verify, **kwargs)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        if response.content:
            # Try to parse JSON, but return raw text if it fails and status is OK
            try:
                return response.json()
            except json.JSONDecodeError:
                if response.ok: # If status is 2xx but not JSON, return text
                    return {"raw_content": response.text}
                raise # Re-raise if not OK and not JSON
        return {} # Return empty dict if no content (e.g. for 202 responses)
    except requests.exceptions.HTTPError as e:
        error_response = e.response
        status_code = error_response.status_code if error_response is not None else "N/A"
        reason = error_response.reason if error_response is not None else "Unknown"
        cli_log.error(f"API Error: {status_code} {reason} for URL: {url}")
        if error_response is not None:
            try:
                error_detail = error_response.json()
                cli_log.error(f"Detail: {error_detail.get('detail', error_response.text)}")
            except json.JSONDecodeError:
                cli_log.error(f"Raw response: {error_response.text}")
            # Specifically handle 403 Forbidden for API key issues
            if error_response.status_code == 403:
                 cli_log.error("Received 403 Forbidden. Check if your API key (--api-key or AKITA_API_KEY) is correct.")
        raise click.Abort()
    except requests.exceptions.ConnectionError as e:
        cli_log.error(f"Connection Error: Could not connect to {url}. Is the node running and API accessible?")
        cli_log.error(f"Details: {e}")
        raise click.Abort()
    except requests.exceptions.Timeout:
        cli_log.error(f"Request Timeout: The request to {url} timed out after {api_timeout}s.")
        raise click.Abort()
    except requests.exceptions.RequestException as e:
        cli_log.error(f"Request Exception: {e} for URL: {url}")
        raise click.Abort()

# --- CLI Group Definition (UPDATED for API Key) ---
@click.group(
    context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 100},
    epilog=CLI_HELP_EPILOG,
)
@click.option(
    '--target-node-api',
    default=f"http://{CLI_DEFAULT_API_HOST}:{CLI_DEFAULT_API_PORT}",
    show_default=True,
    help=f"Base URL of the Akita Genesis node API.",
    envvar="AKITA_TARGET_NODE_API"
)
@click.option( # NEW API Key Option
    '--api-key',
    default=None,
    help="API key for authenticating with the node API.",
    envvar="AKITA_API_KEY" # Allow setting via environment variable
)
@click.option(
    '--ca-bundle',
    default=None,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to a CA bundle used to verify HTTPS certificates.",
    envvar="AKITA_CA_BUNDLE",
)
@click.option(
    '--insecure',
    is_flag=True,
    help="Disable TLS certificate verification for HTTPS API requests.",
)
@click.option(
    '--debug', is_flag=True, help="Enable debug logging for the CLI."
)
@click.pass_context
def cli_app(ctx, target_node_api, api_key, ca_bundle, insecure, debug):
    """
    Akita Genesis Command Line Interface.
    Use this CLI to start nodes, inspect runtime state, and operate the node API.
    """
    ctx.ensure_object(dict)
    ctx.obj['TARGET_NODE_API'] = target_node_api
    ctx.obj['API_KEY'] = api_key # Store API key in context
    ctx.obj['CA_BUNDLE'] = ca_bundle
    ctx.obj['INSECURE'] = insecure
    if debug:
        global cli_log # Modify the global CLI logger instance
        cli_log = setup_cli_logger("akita_cli", level="DEBUG")
        cli_log.debug("CLI debug logging enabled.")
    cli_log.debug(f"Target Node API: {target_node_api}")
    if insecure:
        cli_log.warning("TLS certificate verification is disabled for API requests.")
    if api_key:
        # Mask the key in debug logs for security
        masked_key = f"{'*' * (len(api_key) - 4)}{api_key[-4:]}" if len(api_key) >= 4 else api_key
        cli_log.debug(f"Using API Key: {masked_key}")


@cli_app.command("examples")
@click.option(
    '--topic',
    type=click.Choice(["all"] + list(EXAMPLE_CATALOG.keys()), case_sensitive=False),
    default="all",
    show_default=True,
    help="Show curated examples for one area of the CLI.",
)
def examples_cmd(topic):
    """Print curated examples for common operational workflows."""
    render_examples(topic.lower())

# --- Commands ---

# start command (no API key needed for start itself)
@cli_app.command()
@click.option('--node-name', default=None, help=f"Name for this node.")
@click.option('--cluster-name', default=CLI_DEFAULT_CLUSTER_NAME, show_default=True, help=f"Name of the cluster to join.")
@click.option('--identity-path', default=None, help="Path to the Reticulum identity file.")
@click.option('--api-host', default=CLI_DEFAULT_API_HOST, show_default=True, help=f"Host for the node's API server.")
@click.option('--api-port', default=CLI_DEFAULT_API_PORT, show_default=True, type=int, help=f"Port for the node's API server.")
@click.option('--tls-certfile', default=None, type=click.Path(dir_okay=False, path_type=str), help="PEM certificate file used to serve the control API over HTTPS.")
@click.option('--tls-keyfile', default=None, type=click.Path(dir_okay=False, path_type=str), help="PEM private key file used to serve the control API over HTTPS.")
@click.option('--tls-ca-file', default=None, type=click.Path(dir_okay=False, path_type=str), help="CA bundle used to validate client certificates for mutual TLS.")
@click.option('--tls-require-client-cert', is_flag=True, help="Require trusted client certificates when HTTPS is enabled.")
@click.option('--rns-port', default=None, type=int, help="Specific port for Reticulum Transport.")
@click.option('--capabilities', default=None, multiple=True, help="Node capabilities (repeatable).")
@click.option('--no-api-server', is_flag=True, help="Do not start the HTTP API server.")
@click.option('--log-level', default=CLI_DEFAULT_LOG_LEVEL, show_default=True, type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False), help="Node logging level.")
def start(node_name, cluster_name, identity_path, api_host, api_port, tls_certfile, tls_keyfile, tls_ca_file, tls_require_client_cert, rns_port, capabilities, no_api_server, log_level):
    """
    Starts a new Akita Genesis node process in the foreground.
    """
    from akita_genesis.config.settings import settings as app_settings
    from akita_genesis.core.node import AkitaGenesisNode, run_node_async

    cli_log.info("Preparing to start Akita Genesis node...")
    # Temporarily override log level setting for the node instance being started
    original_settings_log_level = app_settings.LOG_LEVEL
    app_settings.LOG_LEVEL = log_level.upper()
    
    # Create the node instance
    node_instance = AkitaGenesisNode(
        node_name=node_name,
        cluster_name=cluster_name,
        identity_path=identity_path,
        api_host=api_host,
        api_port=api_port,
        api_tls_certfile=tls_certfile,
        api_tls_keyfile=tls_keyfile,
        api_tls_ca_file=tls_ca_file,
        api_tls_require_client_cert=tls_require_client_cert,
        rns_port=rns_port,
        run_api_server=not no_api_server,
        capabilities=list(capabilities) if capabilities else []
    )
    
    cli_log.info(f"Node Name: {node_instance.node_name}, Cluster: {node_instance.cluster_name}, API: {node_instance.api_scheme}://{node_instance.api_host}:{node_instance.api_port}, Caps: {node_instance.capabilities}")
    if no_api_server:
        cli_log.info("API server will NOT be started for this node.")

    # Run the node's main async loop
    try:
        asyncio.run(run_node_async(node_instance))
    except KeyboardInterrupt:
        cli_log.info("Node start command interrupted by user (Ctrl+C). Node should be shutting down.")
    except Exception as e:
        cli_log.error(f"An error occurred while trying to run the node: {e}", exc_info=True)
        cli_log.error("Node startup failed.")
    finally:
        # Restore original log level in case CLI is used further in the same process (unlikely but good practice)
        app_settings.LOG_LEVEL = original_settings_log_level 
        cli_log.info("Node process has exited.")

# status command (API call uses key from context)
@cli_app.command()
@click.option('--json-output', is_flag=True, help="Print the raw JSON response instead of rich panels.")
@click.pass_context
def status(ctx, json_output):
    """Gets the status of the target Akita Genesis node."""
    cli_log.debug(f"Requesting node status from {ctx.obj['TARGET_NODE_API']}")
    result = make_api_request(ctx, "GET", "/status") # make_api_request now handles key
    if json_output:
        Console().print_json(data=result)
        return
    console = Console(theme=log_theme) # Use theme for potential log level colors
    if not result:
        console.print(Panel("[bold red]Failed to retrieve node status or empty response.[/bold red]", title="Error", border_style="red"))
        return

    # Display node status using Rich Panel and Syntax highlighting
    panel_content = Text()
    panel_content.append(f"Node ID: {result.get('node_id', 'N/A')}\n", style="bold cyan")
    panel_content.append(f"Node Name: {result.get('node_name', 'N/A')}\n")
    panel_content.append(f"Capabilities: {', '.join(result.get('capabilities', [])) or 'N/A'}\n")
    panel_content.append(f"Cluster Name: {result.get('cluster_name', 'N/A')}\n")
    panel_content.append(f"RNS Identity: {result.get('rns_identity_hex', 'N/A')}\n")
    panel_content.append(
        f"Status: {result.get('status', 'N/A')}\n",
        style=status_style(result.get('status')),
    )
    panel_content.append(f"Is Leader: {result.get('is_leader', False)}\n", style="bold magenta" if result.get('is_leader') else "")
    panel_content.append(f"Current Leader ID: {result.get('current_leader_id', 'None')}\n")
    panel_content.append(f"Assigned Tasks: {result.get('current_task_count', 0)}\n")
    uptime_s = result.get('uptime_seconds', 0)
    panel_content.append(f"Uptime: {time.strftime('%H:%M:%S', time.gmtime(uptime_s))} (Total {int(uptime_s)}s)\n")
    console.print(Panel(panel_content, title="Node Status", expand=False, border_style="blue"))

    # Display resources if available
    if result.get('resources'):
        resources_json = json.dumps(result['resources'], indent=2)
        console.print(Panel(Syntax(resources_json, "json", theme="native", line_numbers=False, word_wrap=True), title="Resources", border_style="green"))
    
    # Display task counts if available
    if result.get('task_counts'):
        task_counts_table = Table(title="Task Counts")
        task_counts_table.add_column("Status", style="dim")
        task_counts_table.add_column("Count", style="bold")
        for status_key, count_val in result['task_counts'].items():
            task_counts_table.add_row(status_key, str(count_val))
        console.print(task_counts_table)

# cluster commands (API call uses key from context)
@cli_app.group("cluster")
def cluster():
    """Commands related to cluster status and management."""
    pass
@cluster.command("status")
@click.option('--json-output', is_flag=True, help="Print the raw JSON response instead of the formatted table.")
@click.pass_context
def cluster_status(ctx, json_output):
    """Gets the status of the cluster as seen by the target node."""
    cli_log.debug(f"Requesting cluster status from {ctx.obj['TARGET_NODE_API']}")
    result = make_api_request(ctx, "GET", "/cluster/status") # make_api_request handles key
    if json_output:
        Console().print_json(data=result)
        return
    console = Console(theme=log_theme)
    if not result or "nodes" not in result:
        console.print(Panel("[bold red]Failed to retrieve cluster status or malformed response.[/bold red]", title="Error", border_style="red"))
        return

    # Prepare and print cluster status table using Rich
    cluster_name_text = result.get('cluster_name', 'N/A')
    leader_id_text = result.get('current_leader_id', 'None')
    title_text = Text.assemble(
        ("Cluster: ", "bold white"), (cluster_name_text, "bold blue"),
        (" (Leader: ", "bold white"), (leader_id_text if leader_id_text else "None", "bold magenta" if leader_id_text else "default"),
        (")", "bold white")
    )
    table = Table(title=title_text, show_header=True, header_style="bold cyan", expand=True)
    # Add columns to the table
    table.add_column("Node ID", style="cyan", overflow="fold", min_width=12, ratio=1)
    table.add_column("Node Name", overflow="fold", min_width=15, ratio=2)
    table.add_column("Status", style="green", min_width=8, ratio=1)
    table.add_column("Leader", style="magenta", min_width=7, ratio=1)
    table.add_column("Tasks", min_width=5, ratio=1) # For current_task_count
    table.add_column("Caps", overflow="fold", min_width=10, ratio=1) # For capabilities
    table.add_column("RNS Address", overflow="fold", min_width=15, ratio=2)
    table.add_column("Last Seen (UTC)", overflow="fold", min_width=19, ratio=2)
    table.add_column("Resources (CPU%/Mem%)", overflow="fold", min_width=15, ratio=1)

    # Populate table rows
    nodes_data = result.get("nodes", [])
    for node_data in sorted(nodes_data, key=lambda x: x.get("node_name", "")): # Sort by name
        # Determine style based on status
        node_status = normalize_status(node_data.get("status"))
        rendered_status_style = status_style(node_status)
        # Format last seen timestamp
        last_seen_str = "N/A"
        if node_data.get("last_seen"):
            try: last_seen_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(node_data.get("last_seen"))))
            except (ValueError, TypeError): last_seen_str = str(node_data.get("last_seen"))
        # Format resources summary
        resources_str = "N/A"
        if node_data.get("resources"):
            cpu = node_data["resources"].get("cpu", {}).get("percent_used", "N/A")
            mem_virt = node_data["resources"].get("memory", {}).get("virtual", {})
            mem = mem_virt.get("percent_used", "N/A")
            resources_str = f"{cpu}%/{mem}%"
        # Format leader status and capabilities
        is_leader_text = Text("✔", style="bold magenta") if node_data.get("is_leader") else Text("✘", style="dim")
        caps_str = ", ".join(node_data.get("capabilities", [])) or "-"
        
        # Add row to table
        table.add_row(
            node_data.get("node_id", "N/A"), node_data.get("node_name", "N/A"),
            Text(node_data.get("status", "N/A"), style=rendered_status_style),
            is_leader_text, str(node_data.get("current_task_count", 0)), caps_str,
            node_data.get("address_hex", "N/A"), last_seen_str, resources_str
        )
        
    # Print the table or a message if no nodes found
    if not nodes_data:
        console.print(Panel("[yellow]No nodes reported in the cluster by the target node.[/yellow]", title="Cluster Status", expand=False))
    else:
        console.print(table)
    
    # Print summary counts
    summary_text = Text.assemble(
        ("Total Nodes Known: ", "dim"), (str(result.get("total_nodes_known", 0)), "bold"),
        (" | Online: ", "dim"), (str(result.get("online_nodes_count", 0)), "bold green"),
        (" | Degraded: ", "dim"), (str(result.get("degraded_nodes_count", 0)), "bold yellow"),
        (" | Offline: ", "dim"), (str(result.get("offline_nodes_count", 0)), "bold red")
    )
    console.print(summary_text, justify="center")


@cli_app.command("dashboard")
@click.option('--json-output', is_flag=True, help="Print the raw JSON response instead of a rich summary.")
@click.option('--show-events/--hide-events', default=True, show_default=True, help="Show recent ledger events in the dashboard view.")
@click.option('--show-workers/--hide-workers', default=True, show_default=True, help="Show available/busy workers in the dashboard view.")
@click.pass_context
def dashboard_cmd(ctx, json_output, show_events, show_workers):
    """Fetches the high-level control room summary from the target node."""
    result = make_api_request(ctx, "GET", "/dashboard/summary")
    if json_output:
        Console().print_json(data=result)
        return

    console = Console(theme=log_theme)
    node = result.get("node", {})
    cluster_data = result.get("cluster", {})
    tasks_data = result.get("tasks", {})
    security = result.get("security", {})
    panel = Text()
    panel.append(f"Node: {node.get('node_name', 'N/A')} ({node.get('node_id', 'N/A')})\n", style="bold cyan")
    panel.append(f"Cluster: {cluster_data.get('cluster_name', 'N/A')}\n")
    panel.append(f"Status: {node.get('status', 'unknown')}\n", style=status_style(node.get('status')))
    panel.append(f"Leader: {node.get('current_leader_id', 'None')}\n")
    panel.append(f"Uptime: {time.strftime('%H:%M:%S', time.gmtime(node.get('uptime_seconds', 0)))}\n")
    panel.append(f"API Security: {'secured' if security.get('api_secured') else 'open'}\n")
    panel.append(f"UI: {build_ui_url(ctx.obj['TARGET_NODE_API'])}\n", style="bold magenta")
    console.print(Panel(panel, title="Dashboard Summary", border_style="blue", expand=False))

    task_counts = tasks_data.get("counts", {})
    task_table = Table(title="Task Queue Snapshot", expand=True)
    task_table.add_column("Status", style="dim")
    task_table.add_column("Count", style="bold")
    for status_name, count in sorted(task_counts.items()):
        task_table.add_row(status_name, str(count))
    console.print(task_table)

    if show_workers:
        busiest_nodes = cluster_data.get("busiest_nodes", [])
        worker_table = Table(title="Busiest Nodes", expand=True)
        worker_table.add_column("Node")
        worker_table.add_column("Status")
        worker_table.add_column("Tasks")
        worker_table.add_column("Capabilities")
        for worker in busiest_nodes:
            worker_table.add_row(
                worker.get("node_name", worker.get("node_id", "N/A")),
                Text(worker.get("status", "unknown"), style=status_style(worker.get("status"))),
                str(worker.get("current_task_count", 0)),
                ", ".join(worker.get("capabilities", [])) or "-",
            )
        console.print(worker_table)

    if show_events:
        event_table = Table(title="Recent Events", expand=True)
        event_table.add_column("Timestamp", style="green")
        event_table.add_column("Event", style="cyan")
        event_table.add_column("Source")
        for event in result.get("events", []):
            event_table.add_row(
                time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(event.get('timestamp', 0)))) if event.get('timestamp') else 'N/A',
                event.get("event_type", "N/A"),
                event.get("source_node_name", event.get("source_node_id", "N/A")),
            )
        console.print(event_table)

# task commands (API calls use key from context)
@cli_app.group("task")
def task():
    """Commands for task management."""
    pass
@task.command("submit")
@click.argument('task_data_json', type=str)
@click.option('--priority', default=CLI_DEFAULT_TASK_PRIORITY, show_default=True, type=int, help="Task priority.")
@click.pass_context
def task_submit(ctx, task_data_json, priority):
    """
    Submits a new task to the cluster via the target node.
    TASK_DATA_JSON should be a valid JSON string, e.g., '{"action": "compute", "value": 42}'.
    Include 'required_capabilities': [...] in the JSON if needed.
    """
    try:
        task_data = json.loads(task_data_json)
    except json.JSONDecodeError as e:
        cli_log.error(f"Invalid JSON provided for task data: {e}\nInput was: {task_data_json}")
        raise click.Abort()
    cli_log.debug(f"Submitting task to {ctx.obj['TARGET_NODE_API']}: {task_data}, priority: {priority}")
    payload = {"task_data": task_data, "priority": priority}
    result = make_api_request(ctx, "POST", "/tasks/submit", json=payload) # make_api_request handles key
    Console().print_json(data=result)

@task.command("status")
@click.argument('task_id', type=str)
@click.pass_context
def task_status_cmd(ctx, task_id):
    """Gets the status of a specific task."""
    cli_log.debug(f"Requesting status for task {task_id} from {ctx.obj['TARGET_NODE_API']}")
    result = make_api_request(ctx, "GET", f"/tasks/{task_id}") # make_api_request handles key
    Console().print_json(data=result)

@task.command("list")
@click.option('--status', default=None, help="Filter tasks by status (e.g., pending, completed).")
@click.option('--limit', default=10, show_default=True, type=int, help="Number of tasks to list.")
@click.option('--offset', default=0, show_default=True, type=int, help="Offset into the task list for pagination.")
@click.pass_context
def task_list(ctx, status, limit, offset):
    """Lists tasks known to the target node."""
    endpoint = f"/tasks?limit={limit}&offset={offset}"
    if status:
        endpoint += f"&status={status}"
    cli_log.debug(f"Listing tasks from {ctx.obj['TARGET_NODE_API']} with endpoint {endpoint}")
    result = make_api_request(ctx, "GET", endpoint) # make_api_request handles key
    Console().print_json(data=result)


@cli_app.group("config")
def config_group():
    """Commands for inspecting runtime configuration exposed by the node API."""
    pass


@config_group.command("show")
@click.option(
    '--section',
    type=click.Choice(CONFIG_SECTION_CHOICES, case_sensitive=False),
    default=None,
    help="Show only one configuration section.",
)
@click.option('--json-output', is_flag=True, help="Print raw JSON instead of a grouped table.")
@click.pass_context
def config_show(ctx, section, json_output):
    """Fetches the sanitized runtime configuration snapshot from the target node."""
    result = make_api_request(ctx, "GET", "/config")
    if section:
        result = {section: result.get(section, {})}
    if json_output:
        Console().print_json(data=result)
        return

    console = Console(theme=log_theme)
    for section_name, section_values in result.items():
        table = Table(title=f"Config: {section_name}", expand=True)
        table.add_column("Key", style="cyan")
        table.add_column("Value", overflow="fold")
        for key, value in section_values.items():
            if isinstance(value, (dict, list)):
                rendered_value = json.dumps(value)
            else:
                rendered_value = str(value)
            table.add_row(key, rendered_value)
        console.print(table)


@cli_app.group("ui")
def ui_group():
    """Commands related to the browser-based control room."""
    pass


@ui_group.command("url")
@click.option('--open-browser', is_flag=True, help="Open the control room URL in the default browser.")
@click.pass_context
def ui_url(ctx, open_browser):
    """Prints the browser control room URL for the target node."""
    target_url = build_ui_url(ctx.obj['TARGET_NODE_API'])
    click.echo(target_url)
    if open_browser:
        opened = webbrowser.open(target_url)
        if not opened:
            cli_log.warning("The default browser could not be opened automatically.")


@cli_app.group("ddns")
def ddns_group():
    """Optional Akita DDNS integration commands via the external akita_ddns module."""
    pass


@ddns_group.command("doctor")
@click.option('--exec-cmd', default=CLI_DEFAULT_DDNS_EXEC, show_default=True, help="Command used to invoke Akita DDNS.")
@click.option('--config', 'ddns_config', default=CLI_DEFAULT_DDNS_CONFIG, show_default=True, help="Path to akita_config.yaml used by Akita DDNS.")
@click.option('--module-path', default=CLI_DEFAULT_DDNS_MODULE_PATH, show_default=False, help="Path containing the akita_ddns package (prepended to PYTHONPATH).")
@click.option('--timeout', 'timeout_s', default=10, show_default=True, type=int, help="Timeout in seconds for the DDNS diagnostic command.")
def ddns_doctor(exec_cmd, ddns_config, module_path, timeout_s):
    """Checks whether Akita DDNS is installed and callable."""
    try:
        result = _run_ddns_subprocess(exec_cmd, ddns_config, "cli", ["--help"], module_path=module_path, timeout_s=timeout_s)
    except FileNotFoundError:
        raise click.ClickException(
            "Akita DDNS executable was not found. Install Akita DDNS and/or set AKITA_DDNS_EXEC."
        )
    except subprocess.TimeoutExpired:
        raise click.ClickException("Akita DDNS command timed out during diagnostics.")

    _print_subprocess_result(result)
    if result.returncode != 0:
        raise click.ClickException("Akita DDNS CLI check failed.")
    click.echo("Akita DDNS integration is available.")


@ddns_group.command("resolve")
@click.option('--name', required=True, help="DDNS name to resolve, e.g. alpha.prod.")
@click.option('--identity', default=None, help="Optional identity file for the DDNS CLI resolve call.")
@click.option('--timeout', 'resolve_timeout', default=5.0, show_default=True, type=float, help="Resolve timeout passed to Akita DDNS.")
@click.option('--exec-cmd', default=CLI_DEFAULT_DDNS_EXEC, show_default=True, help="Command used to invoke Akita DDNS.")
@click.option('--config', 'ddns_config', default=CLI_DEFAULT_DDNS_CONFIG, show_default=True, help="Path to akita_config.yaml used by Akita DDNS.")
@click.option('--module-path', default=CLI_DEFAULT_DDNS_MODULE_PATH, show_default=False, help="Path containing the akita_ddns package (prepended to PYTHONPATH).")
@click.option('--process-timeout', 'process_timeout_s', default=15, show_default=True, type=int, help="Max seconds to wait for the DDNS subprocess.")
def ddns_resolve(name, identity, resolve_timeout, exec_cmd, ddns_config, module_path, process_timeout_s):
    """Resolves a name through Akita DDNS."""
    args = ["resolve", "--name", name, "--timeout", str(resolve_timeout)]
    if identity:
        args.extend(["--identity", identity])
    try:
        result = _run_ddns_subprocess(exec_cmd, ddns_config, "cli", args, module_path=module_path, timeout_s=process_timeout_s)
    except FileNotFoundError:
        raise click.ClickException("Akita DDNS executable was not found.")
    except subprocess.TimeoutExpired:
        raise click.ClickException("Akita DDNS resolve timed out.")

    _print_subprocess_result(result)
    if result.returncode != 0:
        raise click.ClickException("Akita DDNS resolve failed.")


@ddns_group.command("register")
@click.option('--name', required=True, help="DDNS name to register, e.g. alpha.prod.")
@click.option('--rid', default=None, help="RID to register. If omitted, Akita DDNS uses its local default identity RID.")
@click.option('--ttl', default=3600, show_default=True, type=int, help="Registration TTL in seconds.")
@click.option('--identity', default=None, help="Optional identity file for signing the registration.")
@click.option('--exec-cmd', default=CLI_DEFAULT_DDNS_EXEC, show_default=True, help="Command used to invoke Akita DDNS.")
@click.option('--config', 'ddns_config', default=CLI_DEFAULT_DDNS_CONFIG, show_default=True, help="Path to akita_config.yaml used by Akita DDNS.")
@click.option('--module-path', default=CLI_DEFAULT_DDNS_MODULE_PATH, show_default=False, help="Path containing the akita_ddns package (prepended to PYTHONPATH).")
@click.option('--process-timeout', 'process_timeout_s', default=15, show_default=True, type=int, help="Max seconds to wait for the DDNS subprocess.")
def ddns_register(name, rid, ttl, identity, exec_cmd, ddns_config, module_path, process_timeout_s):
    """Registers a name through Akita DDNS."""
    args = ["register", "--name", name, "--ttl", str(ttl)]
    if rid:
        args.extend(["--rid", rid])
    if identity:
        args.extend(["--identity", identity])
    try:
        result = _run_ddns_subprocess(exec_cmd, ddns_config, "cli", args, module_path=module_path, timeout_s=process_timeout_s)
    except FileNotFoundError:
        raise click.ClickException("Akita DDNS executable was not found.")
    except subprocess.TimeoutExpired:
        raise click.ClickException("Akita DDNS register command timed out.")

    _print_subprocess_result(result)
    if result.returncode != 0:
        raise click.ClickException("Akita DDNS register failed.")


@ddns_group.command("register-node")
@click.option('--name', default=None, help="DDNS name override. Defaults to normalized node_name.cluster_name.")
@click.option('--ttl', default=3600, show_default=True, type=int, help="Registration TTL in seconds.")
@click.option('--identity', default=None, help="Optional DDNS signer identity path.")
@click.option('--exec-cmd', default=CLI_DEFAULT_DDNS_EXEC, show_default=True, help="Command used to invoke Akita DDNS.")
@click.option('--config', 'ddns_config', default=CLI_DEFAULT_DDNS_CONFIG, show_default=True, help="Path to akita_config.yaml used by Akita DDNS.")
@click.option('--module-path', default=CLI_DEFAULT_DDNS_MODULE_PATH, show_default=False, help="Path containing the akita_ddns package (prepended to PYTHONPATH).")
@click.option('--process-timeout', 'process_timeout_s', default=15, show_default=True, type=int, help="Max seconds to wait for the DDNS subprocess.")
@click.pass_context
def ddns_register_node(ctx, name, ttl, identity, exec_cmd, ddns_config, module_path, process_timeout_s):
    """Registers the current Akita node RID (from /status) into Akita DDNS."""
    status = make_api_request(ctx, "GET", "/status")
    rid = status.get("rns_identity_hex")
    if not rid:
        raise click.ClickException("Node status did not include rns_identity_hex; cannot register with DDNS.")

    derived_name = name
    if not derived_name:
        node_name = normalize_ddns_name(str(status.get("node_name", "akita-node")))
        cluster_name = normalize_ddns_name(str(status.get("cluster_name", "default")))
        derived_name = f"{node_name}.{cluster_name}" if cluster_name else node_name

    args = ["register", "--name", derived_name, "--rid", rid, "--ttl", str(ttl)]
    if identity:
        args.extend(["--identity", identity])

    try:
        result = _run_ddns_subprocess(exec_cmd, ddns_config, "cli", args, module_path=module_path, timeout_s=process_timeout_s)
    except FileNotFoundError:
        raise click.ClickException("Akita DDNS executable was not found.")
    except subprocess.TimeoutExpired:
        raise click.ClickException("Akita DDNS register-node command timed out.")

    _print_subprocess_result(result)
    if result.returncode != 0:
        raise click.ClickException("Akita DDNS register-node failed.")


@ddns_group.command("list")
@click.option('--registry/--no-registry', default=True, show_default=True, help="Include DDNS registry data.")
@click.option('--namespaces/--no-namespaces', default=True, show_default=True, help="Include DDNS namespaces data.")
@click.option('--reputation/--no-reputation', default=False, show_default=True, help="Include DDNS reputation data.")
@click.option('--exec-cmd', default=CLI_DEFAULT_DDNS_EXEC, show_default=True, help="Command used to invoke Akita DDNS.")
@click.option('--config', 'ddns_config', default=CLI_DEFAULT_DDNS_CONFIG, show_default=True, help="Path to akita_config.yaml used by Akita DDNS.")
@click.option('--module-path', default=CLI_DEFAULT_DDNS_MODULE_PATH, show_default=False, help="Path containing the akita_ddns package (prepended to PYTHONPATH).")
@click.option('--process-timeout', 'process_timeout_s', default=15, show_default=True, type=int, help="Max seconds to wait for the DDNS subprocess.")
def ddns_list(registry, namespaces, reputation, exec_cmd, ddns_config, module_path, process_timeout_s):
    """Displays local persisted DDNS state via Akita DDNS CLI."""
    args = ["list"]
    if registry:
        args.append("--registry")
    if namespaces:
        args.append("--namespaces")
    if reputation:
        args.append("--reputation")

    try:
        result = _run_ddns_subprocess(exec_cmd, ddns_config, "cli", args, module_path=module_path, timeout_s=process_timeout_s)
    except FileNotFoundError:
        raise click.ClickException("Akita DDNS executable was not found.")
    except subprocess.TimeoutExpired:
        raise click.ClickException("Akita DDNS list command timed out.")

    _print_subprocess_result(result)
    if result.returncode != 0:
        raise click.ClickException("Akita DDNS list failed.")

# ledger_group commands (API call uses key from context)
@cli_app.group("ledger")
def ledger_group():
    """Commands for interacting with the ledger."""
    pass
@ledger_group.command("view")
@click.option('--limit', default=20, type=int, help="Number of entries.")
@click.option('--offset', default=0, type=int, help="Offset for pagination.")
@click.option('--event-type', default=None, help="Filter by event type.")
@click.pass_context
def ledger_view(ctx, limit, offset, event_type):
    """Views ledger entries from the target node."""
    endpoint = f"/ledger?limit={limit}&offset={offset}"
    if event_type:
        endpoint += f"&event_type={event_type}"
    cli_log.debug(f"Fetching ledger entries from {ctx.obj['TARGET_NODE_API']} with endpoint {endpoint}")
    result = make_api_request(ctx, "GET", endpoint) # make_api_request handles key
    console = Console(theme=log_theme)
    if not result or "events" not in result:
        console.print(Panel("[bold red]Failed to retrieve ledger entries or malformed response.[/bold red]", title="Error", border_style="red"))
        return
    events = result.get("events", [])
    if not events:
        console.print(Panel("[yellow]No ledger entries found matching criteria.[/yellow]", title="Ledger View", expand=False))
        return
    # Prepare and print ledger table
    table = Table(title="Ledger Entries", show_header=True, header_style="bold blue", expand=True)
    table.add_column("ID", style="dim", width=6)
    table.add_column("Timestamp (UTC)", style="green", width=20)
    table.add_column("Event Type", style="cyan", width=20)
    table.add_column("Source Node ID", width=15, overflow="fold")
    table.add_column("Source Node Name", width=15, overflow="fold")
    table.add_column("Cluster", width=15, overflow="fold")
    table.add_column("Details", overflow="fold")
    # Populate table rows
    for event in events:
        ts_str = "N/A"
        if event.get("timestamp"):
            try: ts_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(event.get("timestamp"))))
            except (ValueError, TypeError): ts_str = str(event.get("timestamp"))
        details_str = json.dumps(event.get("details", {}), indent=None, separators=(',', ':')) 
        if len(details_str) > 70: details_str = details_str[:67] + "..." # Truncate long details
        table.add_row(
            str(event.get("id", "N/A")), ts_str, event.get("event_type", "N/A"),
            event.get("source_node_id", "N/A"), event.get("source_node_name", "N/A"),
            event.get("cluster_name", "N/A"), details_str
        )
    console.print(table)

# logs command (API call uses key from context)
@cli_app.command("logs")
@click.option('--limit', default=100, type=int, help="Number of log entries to fetch.")
@click.option('--level', default=None, help="Filter logs by level (e.g., INFO, ERROR) - if supported by node.")
@click.option('--follow', '-f', is_flag=True, help="Follow logs in real-time by polling the node API.")
@click.pass_context
def logs_cmd(ctx, limit, level, follow):
    """Fetches logs from the target node."""
    console = Console(theme=log_theme) # Apply theme for highlighting
    log_highlighter = LogHighlighter()

    endpoint = f"/logs?limit={limit}"
    if level:
        endpoint += f"&level={level.upper()}"

    if follow:
        console.print("[green]Following logs (polling every 1s). Press Ctrl+C to stop.[/green]")
        last_ts = 0.0
        try:
            while True:
                poll_result = make_api_request(ctx, "GET", endpoint)
                entries = poll_result.get("logs", [])
                # Normalize and print only new entries
                for entry in entries:
                    if isinstance(entry, dict):
                        ts = entry.get("timestamp")
                        # Accept numeric timestamps or string timestamps
                        try:
                            ts_val = float(ts)
                        except Exception:
                            try:
                                ts_val = time.mktime(time.strptime(str(ts), "%Y-%m-%d %H:%M:%S"))
                            except Exception:
                                ts_val = time.time()
                        if ts_val <= last_ts:
                            continue
                        last_ts = max(last_ts, ts_val)
                        level_name = (entry.get("level") or "INFO").upper()
                        ts_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts_val))
                        msg = f"{ts_str} [{level_name}] {entry.get('logger','')}: {entry.get('message','')}"
                        console.print(log_highlighter(msg), style=f"log.level.{level_name}")
                    else:
                        # Fallback for legacy string-format logs
                        console.print(log_highlighter(entry))
                time.sleep(1.0)
        except KeyboardInterrupt:
            console.print("[yellow]Stopped following logs.[/yellow]")
            return
    
    cli_log.debug(f"Fetching logs from {ctx.obj['TARGET_NODE_API']} with endpoint {endpoint}")
    result = make_api_request(ctx, "GET", endpoint) # make_api_request handles key
    
    if not result or "logs" not in result:
        console.print(Panel("[bold red]Failed to retrieve logs or malformed response.[/bold red]", title="Error", border_style="red"))
        return

    log_entries = result.get("logs", [])
    node_id_logs = result.get("node_id", "Unknown Node")

    if not log_entries:
        console.print(Panel(f"[yellow]No log entries returned from node {node_id_logs}.[/yellow]", title=f"Logs from {node_id_logs}", expand=False))
        return

    # Print logs with highlighting (support structured entries)
    console.print(Panel(f"Displaying last {len(log_entries)} log entries from node [bold cyan]{node_id_logs}[/bold cyan]", title="Node Logs"))
    for entry in log_entries:
        if isinstance(entry, dict):
            ts = entry.get('timestamp')
            if isinstance(ts, (int, float)):
                ts_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))
            else:
                ts_str = str(ts)
            level_name = (entry.get('level') or 'INFO').upper()
            msg = f"{ts_str} [{level_name}] {entry.get('logger','')}: {entry.get('message','')}"
            style = f"log.level.{level_name}" if level_name else ""
            console.print(log_highlighter(msg), style=style)
        else:
            level_match = re.search(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]", entry)
            style = ""
            if level_match:
                level_name = level_match.group(1)
                style = f"log.level.{level_name}"
            console.print(log_highlighter(entry), style=style)


@cli_app.command("health")
@click.option('--json-output', is_flag=True, help="Print the raw dashboard JSON used for the health snapshot.")
@click.pass_context
def health_cmd(ctx, json_output):
    """Prints a concise health summary for scripting and operator spot checks."""
    result = make_api_request(ctx, "GET", "/readyz")
    if json_output:
        Console().print_json(data=result)
        return

    checks = result.get("checks", {})
    click.echo(
        f"node={result.get('node_name','unknown')} ready={result.get('ready', False)} "
        f"status={result.get('status','unknown')} leader={result.get('current_leader_id','none')} "
        f"scheme={result.get('api_scheme','http')} storage={checks.get('storage_initialized', False)} "
        f"transport={checks.get('communication_ready', False)}"
    )


# shutdown command (API call uses key from context)
@cli_app.command()
@click.confirmation_option(prompt='Are you sure you want to shut down the target node?')
@click.pass_context
def shutdown(ctx):
    """Sends a shutdown command to the target Akita Genesis node."""
    cli_log.info(f"Sending shutdown command to {ctx.obj['TARGET_NODE_API']}...")
    try:
        result = make_api_request(ctx, "POST", "/shutdown") # make_api_request handles key
        Console().print_json(data=result)
        cli_log.info("Shutdown command acknowledged by the node.")
    except click.exceptions.Abort: # Handles if user says no to confirmation
        cli_log.info("Shutdown aborted by user.")

# Main entry point for script execution
if __name__ == '__main__':
    # Pass default context object, needed for click commands
    # When run via entry point, click handles context automatically.
    cli_app(obj={}) 
