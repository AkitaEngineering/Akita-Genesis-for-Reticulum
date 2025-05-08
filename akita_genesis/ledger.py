# akita_genesis/modules/ledger.py
import json
import time
from typing import List, Dict, Any, Optional, Union, Awaitable
from enum import Enum
import sqlite3 # For sqlite3.Row type hint if needed, and errors
import asyncio # For asyncio.wrap_future

# Use absolute imports within the package
from akita_genesis.utils.logger import setup_logger
from akita_genesis.config.settings import settings
from akita_genesis.modules.persistence import db_manager # Singleton instance of DatabaseManager

log = setup_logger(__name__)

class EventType(Enum):
    """Enumeration for standard ledger event types."""
    NODE_START = "NODE_START"
    NODE_STOP = "NODE_STOP"
    NODE_ONLINE = "NODE_ONLINE"
    NODE_OFFLINE = "NODE_OFFLINE"
    NODE_RESOURCE_UPDATE = "NODE_RESOURCE_UPDATE" # Potentially verbose
    CLUSTER_JOIN = "CLUSTER_JOIN" # Node added to cluster state
    CLUSTER_LEAVE = "CLUSTER_LEAVE" # Node removed/timed out
    LEADER_ELECTED = "LEADER_ELECTED"
    TASK_SUBMITTED = "TASK_SUBMITTED" # Initial submission to system
    TASK_ACCEPTED = "TASK_ACCEPTED" # Leader acknowledges/queues task
    TASK_ASSIGNED = "TASK_ASSIGNED" # Leader assigns task to worker
    TASK_STARTED = "TASK_STARTED" # Worker acknowledges/starts processing (use WORKER_ACK status)
    TASK_COMPLETED = "TASK_COMPLETED" # Worker reports success
    TASK_FAILED = "TASK_FAILED" # Worker reports failure or leader marks failed (e.g. timeout, max attempts)
    TASK_TIMEOUT = "TASK_TIMEOUT" # Explicit timeout event
    TASK_REQUEUED = "TASK_REQUEUED" # Task put back in queue after failure/timeout
    STATE_SYNC = "STATE_SYNC" # Record of state synchronization events (if needed)
    CONFIG_CHANGE = "CONFIG_CHANGE" # If dynamic config changes are supported
    SECURITY_ALERT = "SECURITY_ALERT"
    GENERAL_INFO = "GENERAL_INFO" # For general informational messages
    ERROR_EVENT = "ERROR_EVENT" # For recording application errors

    def __str__(self):
        # Return the string value of the enum member (e.g., "NODE_START")
        return self.value

class Ledger:
    """
    Manages the system ledger, providing an append-only log of significant events.
    Events are stored in the SQLite database via the DatabaseManager.
    """

    def __init__(self, node_id: Optional[str] = None, node_name: Optional[str] = None, cluster_name: Optional[str] = None):
        """
        Initializes the Ledger.

        Args:
            node_id: The Akita ID of the current node (used as default source).
            node_name: The name of the current node (used as default source).
            cluster_name: The name of the cluster this node belongs to (used as default source).
        """
        self.db = db_manager # Use the global singleton DatabaseManager instance
        self.current_node_id = node_id
        self.current_node_name = node_name
        self.current_cluster_name = cluster_name
        log.info("Ledger module initialized.")

    def set_node_context(self, node_id: str, node_name: str, cluster_name: str):
        """Sets the context (current node info) for events originating locally."""
        self.current_node_id = node_id
        self.current_node_name = node_name
        self.current_cluster_name = cluster_name

    async def record_event(
        self,
        event_type: Union[EventType, str],
        details: Optional[Dict[str, Any]] = None,
        source_node_id: Optional[str] = None,
        source_node_name: Optional[str] = None,
        cluster_name: Optional[str] = None,
        timestamp: Optional[float] = None, # Unix timestamp (float for sub-second precision)
        signature: Optional[str] = None, # Placeholder for future cryptographic signature
    ) -> Optional[int]:
        """
        Asynchronously records an event to the ledger database.

        Args:
            event_type: The type of event (EventType enum member or string).
            details: A dictionary containing event-specific information (will be stored as JSON).
            source_node_id: The Akita ID of the node that originated the event. Defaults to current node's ID.
            source_node_name: The name of the originating node. Defaults to current node's name.
            cluster_name: The cluster associated with the event. Defaults to current node's cluster.
            timestamp: Optional event timestamp (unix epoch float). Defaults to `time.time()`.
            signature: Optional cryptographic signature for the event (future use).

        Returns:
            The integer ID (rowid) of the inserted ledger entry if successful, otherwise None.
        """
        # Convert event type enum to string if necessary
        event_type_str = str(event_type)
        # Serialize details dictionary to JSON string, handle None case
        details_json = json.dumps(details) if details is not None else "{}"
        # Use provided timestamp or get current time
        event_timestamp = timestamp if timestamp is not None else time.time()

        # Determine final source info, using current node context as default
        final_source_node_id = source_node_id or self.current_node_id
        final_source_node_name = source_node_name or self.current_node_name
        final_cluster_name = cluster_name or self.current_cluster_name
        
        # Basic check: Ensure a source node ID is present (important for tracking origin)
        if not final_source_node_id:
            log.warning(f"Attempted to record event '{event_type_str}' without a source_node_id. Event not recorded.")
            # Depending on strictness, could raise an error or allow 'SYSTEM' as source_node_id
            return None

        # Prepare SQL statement and parameters
        sql = """
            INSERT INTO ledger (timestamp, event_type, source_node_id, source_node_name, cluster_name, details, signature)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            event_timestamp,
            event_type_str,
            final_source_node_id,
            final_source_node_name,
            final_cluster_name,
            details_json,
            signature, # Store signature if provided
        )

        try:
            # Execute the insert operation via the DatabaseManager's queue
            # db_manager.execute returns a concurrent.futures.Future
            future = self.db.execute(sql, params)
            # Use asyncio.wrap_future to await the result from the DB thread
            row_id = await asyncio.wrap_future(future) 
            
            # Log successful recording (debug level to avoid excessive logging)
            log.debug(f"Recorded event: {event_type_str}, Details: {details_json[:100]}..., ID: {row_id}")
            # Return the ID of the newly inserted row
            return row_id
        except Exception as e:
            # Log any errors during the database operation
            log.error(f"Failed to record event '{event_type_str}' to ledger: {e}", exc_info=True)
            return None

    async def get_events(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: Optional[Union[EventType, str]] = None,
        source_node_id: Optional[str] = None,
        start_time: Optional[float] = None, # Unix epoch float
        end_time: Optional[float] = None,   # Unix epoch float
    ) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves events from the ledger, with optional filtering and pagination.

        Args:
            limit: Maximum number of events to retrieve.
            offset: Number of events to skip from the beginning (for pagination).
            event_type: Filter by specific event type (EventType enum or string).
            source_node_id: Filter by source node Akita ID.
            start_time: Filter events occurring at or after this timestamp.
            end_time: Filter events occurring at or before this timestamp.

        Returns:
            A list of event dictionaries (each representing a ledger row), 
            ordered by timestamp descending. Returns an empty list on error or if no events match.
        """
        # Base SQL query
        base_sql = "SELECT id, timestamp, event_type, source_node_id, source_node_name, cluster_name, details, signature FROM ledger"
        # List to hold WHERE clause conditions and parameters
        conditions = []
        params: List[Any] = []

        # Build WHERE clause dynamically based on provided filters
        if event_type:
            conditions.append("event_type = ?")
            params.append(str(event_type))
        if source_node_id:
            conditions.append("source_node_id = ?")
            params.append(source_node_id)
        if start_time is not None: # Check for None explicitly
            conditions.append("timestamp >= ?")
            params.append(start_time)
        if end_time is not None:
            conditions.append("timestamp <= ?")
            params.append(end_time)

        # Append WHERE clause if any conditions were added
        if conditions:
            base_sql += " WHERE " + " AND ".join(conditions)

        # Add ordering, limit, and offset for pagination
        base_sql += " ORDER BY timestamp DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        try:
            # Execute the select query via the DatabaseManager
            future = self.db.execute(base_sql, tuple(params))
            # Await the result (list of rows)
            rows = await asyncio.wrap_future(future)
            
            # Process the results
            events = []
            if rows: # Check if rows is not None and not empty
                for row in rows:
                    # Convert sqlite3.Row object to a dictionary
                    event = dict(row) 
                    # Attempt to parse the 'details' JSON string back into a dictionary
                    try:
                        # Handle cases where details might be None or empty string before parsing
                        event['details'] = json.loads(event['details']) if event.get('details') else {}
                    except json.JSONDecodeError:
                        # If JSON parsing fails, log a warning and store the raw string with an error indicator
                        log.warning(f"Could not parse details JSON for ledger event ID {event.get('id')}: {event.get('details')}")
                        event['details'] = {"error": "invalid_json", "raw": event.get('details')}
                    events.append(event)
            return events
        except Exception as e:
            # Log errors during event retrieval
            log.error(f"Failed to retrieve ledger events: {e}", exc_info=True)
            return [] # Return empty list on error

    async def get_event_by_id(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Asynchronously retrieves a single ledger event by its unique ID."""
        sql = "SELECT id, timestamp, event_type, source_node_id, source_node_name, cluster_name, details, signature FROM ledger WHERE id = ?"
        try:
            future = self.db.execute(sql, (event_id,))
            rows = await asyncio.wrap_future(future)
            # Check if a row was found
            if rows and rows[0]:
                event = dict(rows[0]) # Convert row to dict
                # Parse details JSON
                try:
                    event['details'] = json.loads(event['details']) if event.get('details') else {}
                except json.JSONDecodeError:
                    event['details'] = {"error": "invalid_json", "raw": event.get('details')}
                return event
            else:
                # Event with the given ID was not found
                return None
        except Exception as e:
            log.error(f"Failed to retrieve event by ID {event_id}: {e}", exc_info=True)
            return None

# Note: The future_to_task helper used previously is replaced by asyncio.wrap_future,
# which is the standard way to await concurrent.futures.Future objects since Python 3.7+.


