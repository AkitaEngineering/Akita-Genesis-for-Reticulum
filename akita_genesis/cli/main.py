# akita_genesis/cli/main.py
import asyncio
import json
import click
import requests 
import platform
import os
import sys
import time 
import re # For log parsing

# Rich for pretty CLI output
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.syntax import Syntax
from rich.live import Live # For potential live log tailing
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

# Ensure the project root is in the Python path for direct execution from project root
# This is mainly for development. When installed as a package, imports should work directly.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Use absolute imports when running as a module/package
from akita_genesis.config.settings import settings
from akita_genesis.core.node import AkitaGenesisNode, run_node_async 
from akita_genesis.utils.logger import setup_logger 

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
cli_log = setup_logger("akita_cli", level="INFO")

# --- API Request Helper (UPDATED for API Key) ---
def make_api_request(ctx, method: str, endpoint: str, **kwargs) -> dict:
    """
    Helper function to make requests to the node's API.
    Uses target_node_api and api_key from click context.
    """
    base_url = ctx.obj.get('TARGET_NODE_API')
    api_key = ctx.obj.get('API_KEY') # Get API key from context
    if not base_url:
        cli_log.error("Target node API URL is not set. Use --target-node-api <URL>.")
        raise click.Abort()

    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    cli_log.debug(f"Making API {method} request to: {url}")
    
    headers = kwargs.pop('headers', {}) # Get existing headers or create new dict
    if api_key:
        headers[settings.API_KEY_HEADER_NAME] = api_key
        cli_log.debug(f"Using API Key ending in '...{api_key[-4:] if len(api_key) >= 4 else api_key}'")
    elif settings.VALID_API_KEYS: # Warn if keys are configured but none provided
         cli_log.warning(f"No API key provided (--api-key or AKITA_API_KEY). Request to {url} might fail if API is secured.")
         
    try:
        response = requests.request(method, url, headers=headers, timeout=settings.CONTROL_API_TIMEOUT_S, **kwargs)
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
        cli_log.error(f"API Error: {e.response.status_code} {e.response.reason} for URL: {url}")
        try:
            error_detail = e.response.json()
            cli_log.error(f"Detail: {error_detail.get('detail', e.response.text)}")
        except json.JSONDecodeError:
            cli_log.error(f"Raw response: {e.response.text}")
        # Specifically handle 403 Forbidden for API key issues
        if e.response.status_code == 403:
             cli_log.error("Received 403 Forbidden. Check if your API key (--api-key or AKITA_API_KEY) is correct.")
        raise click.Abort()
    except requests.exceptions.ConnectionError as e:
        cli_log.error(f"Connection Error: Could not connect to {url}. Is the node running and API accessible?")
        cli_log.error(f"Details: {e}")
        raise click.Abort()
    except requests.exceptions.Timeout:
        cli_log.error(f"Request Timeout: The request to {url} timed out after {settings.CONTROL_API_TIMEOUT_S}s.")
        raise click.Abort()
    except requests.exceptions.RequestException as e:
        cli_log.error(f"Request Exception: {e} for URL: {url}")
        raise click.Abort()

# --- CLI Group Definition (UPDATED for API Key) ---
@click.group()
@click.option(
    '--target-node-api',
    default=f"http://{settings.DEFAULT_API_HOST}:{settings.DEFAULT_API_PORT}",
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
    '--debug', is_flag=True, help="Enable debug logging for the CLI."
)
@click.pass_context
def cli_app(ctx, target_node_api, api_key, debug):
    """
    Akita Genesis Command Line Interface.
    Use this CLI to start nodes and interact with their APIs.
    """
    ctx.ensure_object(dict)
    ctx.obj['TARGET_NODE_API'] = target_node_api
    ctx.obj['API_KEY'] = api_key # Store API key in context
    if debug:
        global cli_log # Modify the global CLI logger instance
        cli_log = setup_logger("akita_cli", level="DEBUG")
        cli_log.debug("CLI debug logging enabled.")
    cli_log.debug(f"Target Node API: {target_node_api}")
    if api_key:
        # Mask the key in debug logs for security
        masked_key = f"{'*' * (len(api_key) - 4)}{api_key[-4:]}" if len(api_key) >= 4 else api_key
        cli_log.debug(f"Using API Key: {masked_key}")

# --- Commands ---

# start command (no API key needed for start itself)
@cli_app.command()
@click.option('--node-name', default=None, help=f"Name for this node.")
@click.option('--cluster-name', default=settings.DEFAULT_CLUSTER_NAME, help=f"Name of the cluster to join.")
@click.option('--identity-path', default=None, help="Path to the Reticulum identity file.")
@click.option('--api-host', default=settings.DEFAULT_API_HOST, help=f"Host for the node's API server.")
@click.option('--api-port', default=settings.DEFAULT_API_PORT, type=int, help=f"Port for the node's API server.")
@click.option('--rns-port', default=None, type=int, help="Specific port for Reticulum Transport.")
@click.option('--capabilities', default=None, multiple=True, help="Node capabilities (repeatable).")
@click.option('--no-api-server', is_flag=True, help="Do not start the HTTP API server.")
@click.option('--log-level', default=settings.LOG_LEVEL, type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False), help="Node logging level.")
def start(node_name, cluster_name, identity_path, api_host, api_port, rns_port, capabilities, no_api_server, log_level):
    """
    Starts a new Akita Genesis node process in the foreground.
    """
    cli_log.info("Preparing to start Akita Genesis node...")
    # Temporarily override log level setting for the node instance being started
    original_settings_log_level = settings.LOG_LEVEL
    settings.LOG_LEVEL = log_level.upper()
    
    # Create the node instance
    node_instance = AkitaGenesisNode(
        node_name=node_name,
        cluster_name=cluster_name,
        identity_path=identity_path,
        api_host=api_host,
        api_port=api_port,
        rns_port=rns_port,
        run_api_server=not no_api_server,
        capabilities=list(capabilities) if capabilities else []
    )
    
    cli_log.info(f"Node Name: {node_instance.node_name}, Cluster: {node_instance.cluster_name}, API: http://{node_instance.api_host}:{node_instance.api_port}, Caps: {node_instance.capabilities}")
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
        settings.LOG_LEVEL = original_settings_log_level 
        cli_log.info("Node process has exited.")

# status command (API call uses key from context)
@cli_app.command()
@click.pass_context
def status(ctx):
    """Gets the status of the target Akita Genesis node."""
    cli_log.debug(f"Requesting node status from {ctx.obj['TARGET_NODE_API']}")
    result = make_api_request(ctx, "GET", "/status") # make_api_request now handles key
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
    panel_content.append(f"Status: {result.get('status', 'N/A')}\n", style="bold green" if result.get('status') == "ONLINE" else "bold yellow")
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
@click.pass_context
def cluster_status(ctx):
    """Gets the status of the cluster as seen by the target node."""
    cli_log.debug(f"Requesting cluster status from {ctx.obj['TARGET_NODE_API']}")
    result = make_api_request(ctx, "GET", "/cluster/status") # make_api_request handles key
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
        status_style = "green"
        if node_data.get("status") == "OFFLINE": status_style = "red"
        elif node_data.get("status") == "DEGRADED": status_style = "yellow"
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
            Text(node_data.get("status", "N/A"), style=status_style),
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
        (" | Online: ", "dim"), (str(result.get("online_nodes_count", 0)), "bold green")
    )
    console.print(summary_text, justify="center")

# task commands (API calls use key from context)
@cli_app.group("task")
def task():
    """Commands for task management."""
    pass
@task.command("submit")
@click.argument('task_data_json', type=str)
@click.option('--priority', default=settings.DEFAULT_TASK_PRIORITY, type=int, help="Task priority.")
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
@click.option('--limit', default=10, type=int, help="Number of tasks to list.")
@click.pass_context
def task_list(ctx, status, limit):
    """Lists tasks known to the target node."""
    endpoint = f"/tasks?limit={limit}"
    if status:
        endpoint += f"&status={status}"
    cli_log.debug(f"Listing tasks from {ctx.obj['TARGET_NODE_API']} with endpoint {endpoint}")
    result = make_api_request(ctx, "GET", endpoint) # make_api_request handles key
    Console().print_json(data=result)

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
@click.option('--follow', '-f', is_flag=True, help="Follow logs in real-time (not implemented yet).")
@click.pass_context
def logs_cmd(ctx, limit, level, follow):
    """Fetches logs from the target node."""
    console = Console(theme=log_theme) # Apply theme for highlighting
    log_highlighter = LogHighlighter()

    if follow:
        console.print("[yellow]Log following (-f) is not yet implemented. Fetching recent logs instead.[/yellow]")
        # TODO: Implement log following using WebSockets or Server-Sent Events if API supports it.

    endpoint = f"/logs?limit={limit}"
    if level:
        endpoint += f"&level={level.upper()}"
    
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

    # Print logs with highlighting
    console.print(Panel(f"Displaying last {len(log_entries)} log entries from node [bold cyan]{node_id_logs}[/bold cyan]", title="Node Logs"))
    for entry in log_entries:
        # Basic coloring for log levels if they are present in the string
        level_match = re.search(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]", entry)
        style = ""
        if level_match:
            level_name = level_match.group(1)
            style = f"log.level.{level_name}" # Use theme style like log.level.INFO
        
        # Print with highlighter and potential level style
        console.print(log_highlighter(entry), style=style)


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
