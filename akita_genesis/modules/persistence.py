# akita_genesis/modules/persistence.py
import sqlite3
import threading
import queue # Using standard library queue
import time
from pathlib import Path
from typing import Any, List, Tuple, Dict, Callable, Optional
from concurrent.futures import Future # For returning results from queued operations

# Use absolute imports within the package
from akita_genesis.config.settings import settings
from akita_genesis.utils.logger import setup_logger

log = setup_logger(__name__)

# Type for database operations: (sql_statement, parameters, callback_for_result, future_to_set)
DbOperation = Tuple[str, Tuple[Any, ...], Optional[Callable[[Any], None]], Future]

class DatabaseManager:
    """
    Manages SQLite database connections and operations in a thread-safe manner.
    It uses a dedicated thread to process database write operations sequentially.
    Read operations can be performed more concurrently if needed, but for simplicity,
    all operations can go through the queue.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        # Singleton pattern: Ensure only one instance exists
        if not cls._instance:
            with cls._lock:
                # Double-check locking
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_path: Optional[Path] = None):
        # Ensure __init__ runs only once for the singleton instance
        if not hasattr(self, '_initialized'): 
            self.db_path = db_path or settings.SQLITE_DB_FILE
            # Ensure the directory for the database file exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            self._db_queue = queue.Queue() # Queue for all DB operations
            self._db_thread_stop_event = threading.Event() # Event to signal thread termination
            # Create and start the dedicated database processing thread
            self._db_thread = threading.Thread(target=self._process_queue, daemon=True)
            self._db_thread.name = "AkitaDBThread" # Name the thread for easier debugging
            self._db_thread.start()
            
            # Initialize DB schema asynchronously; store the future for potential waiting
            self.init_db_future = self.init_db() 
            self._initialized = True
            log.info(f"DatabaseManager initialized with DB at {self.db_path}. Schema init queued.")

    def _get_connection(self) -> sqlite3.Connection:
        """Gets a new database connection. Intended to be called within the DB thread."""
        try:
            # Connect to the SQLite database file. 
            # timeout is for lock acquisition. check_same_thread=False is not needed as all writes happen in one thread.
            conn = sqlite3.connect(self.db_path, timeout=10.0) 
            # Set row_factory to access columns by name (like a dictionary)
            conn.row_factory = sqlite3.Row 
            # Enable Write-Ahead Logging for better concurrency (allows readers while writing)
            conn.execute("PRAGMA journal_mode=WAL;") 
            # Enforce foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON;")
            return conn
        except sqlite3.Error as e:
            log.error(f"Error connecting to database {self.db_path}: {e}")
            raise # Re-raise the exception

    def _process_queue(self):
        """The main loop for the dedicated database thread. Processes operations from the queue."""
        conn = self._get_connection() # Get initial connection for this thread
        log.info("Database processing thread started.")
        
        while not self._db_thread_stop_event.is_set():
            op_future = None # Ensure op_future is defined in outer scope
            try:
                # Wait for an operation from the queue (with timeout to allow checking stop event)
                sql, params, callback, op_future = self._db_queue.get(timeout=1.0)
                
                # Check for a special sentinel value used for graceful shutdown
                if sql == "STOP_THREAD_SENTINEL": 
                    op_future.set_result(True) # Signal completion of the sentinel processing
                    break # Exit the loop

                cursor = conn.cursor()
                try:
                    # Execute the SQL statement with parameters
                    cursor.execute(sql, params)
                    
                    # Determine the result and commit if it's a write operation
                    sql_upper_stripped = sql.strip().upper()
                    if sql_upper_stripped.startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER")):
                        conn.commit() # Commit changes for write operations
                        # For INSERT, result is lastrowid; for others, it's rowcount
                        result = cursor.lastrowid if sql_upper_stripped.startswith("INSERT") else cursor.rowcount
                    else: # Read operation (SELECT)
                        result = cursor.fetchall() # Fetch all results
                    
                    # If a Future was provided, set its result
                    if op_future: 
                        op_future.set_result(result)
                    # If a synchronous callback was provided, execute it
                    if callback: 
                        try:
                            callback(result)
                        except Exception as cb_e:
                            log.error(f"Error in DB operation callback for SQL '{sql[:50]}...': {cb_e}")

                except sqlite3.Error as e:
                    # Handle database errors (syntax errors, constraint violations, etc.)
                    log.error(f"Database error executing '{sql[:50]}...': {e}. Params: {params}")
                    conn.rollback() # Rollback transaction on error
                    if op_future: # Set exception on the Future if provided
                        op_future.set_exception(e)
                except Exception as e: # Catch any other unexpected errors during execution
                    log.error(f"Unexpected error during DB operation '{sql[:50]}...': {e}", exc_info=True)
                    if op_future:
                        op_future.set_exception(e)
                finally:
                    cursor.close() # Ensure cursor is always closed
                    self._db_queue.task_done() # Mark task as done in the queue

            except queue.Empty:
                # Queue was empty during timeout, just continue waiting
                continue 
            except Exception as e: # Catch errors in the queue processing loop itself (e.g., connection issues)
                log.error(f"Critical error in database processing thread: {e}", exc_info=True)
                # Attempt to re-establish DB connection if it seems problematic
                try: conn.close()
                except: pass # Ignore errors closing potentially broken connection
                try:
                    conn = self._get_connection() # Try to get a new connection
                except Exception as conn_e:
                    log.error(f"Failed to re-establish DB connection: {conn_e}. DB thread stopping.")
                    # If connection fails critically, signal error on pending future and stop thread
                    if op_future and not op_future.done(): 
                         op_future.set_exception(conn_e) 
                    self._db_thread_stop_event.set() # Stop the thread
                    break # Exit the loop
        
        # Cleanly close the connection when the loop exits
        conn.close()
        log.info("Database processing thread stopped.")

    def execute(self, sql: str, params: Tuple[Any, ...] = (), 
                callback: Optional[Callable[[Any], None]] = None) -> Future:
        """
        Queues a database operation for execution in the dedicated DB thread.

        Args:
            sql: The SQL statement to execute.
            params: A tuple of parameters to substitute into the SQL statement.
            callback: An optional synchronous function to call with the result (use with caution).

        Returns:
            A concurrent.futures.Future object. The caller can await this 
            (using asyncio.wrap_future) to get the result or exception asynchronously.
        """
        op_future = Future() # Create a Future to track this operation
        # Put the operation details onto the queue for the DB thread to process
        self._db_queue.put((sql, params, callback, op_future))
        return op_future

    def init_db(self) -> Future:
        """
        Queues the database schema initialization statements.
        Creates tables and indexes if they don't already exist.

        Returns:
            A Future that resolves when the *last* schema statement has been processed.
            Note: This doesn't guarantee all statements succeeded, only that they were processed.
                  Check logs for errors.
        """
        # Define all schema creation/verification statements
        schema_statements = [
            """
            CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), -- High-precision timestamp
                event_type TEXT NOT NULL,
                source_node_id TEXT,      -- Akita Node ID
                source_node_name TEXT,
                cluster_name TEXT,
                details TEXT,             -- JSON string for flexible data
                signature TEXT            -- Placeholder for cryptographic signature
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_ledger_timestamp ON ledger (timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_ledger_event_type ON ledger (event_type);",
            "CREATE INDEX IF NOT EXISTS idx_ledger_source_node_id ON ledger (source_node_id);",
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,                 -- Task UUID
                submit_time REAL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                priority INTEGER DEFAULT 10,
                data TEXT NOT NULL,                  -- JSON representation of task payload
                status TEXT NOT NULL DEFAULT 'pending', -- e.g., pending, accepted, assigned, processing, completed, failed
                assigned_to_node_id TEXT,            -- Akita Node ID of assigned worker
                result TEXT,                         -- JSON result or error message
                last_updated REAL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                submitted_by_node_id TEXT,           -- Akita Node ID of original submitter
                execution_attempts INTEGER DEFAULT 0 -- Number of times task assignment was attempted
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_tasks_status_priority ON tasks (status, priority);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_assigned_to_node_id ON tasks (assigned_to_node_id);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_submitted_by_node_id ON tasks (submitted_by_node_id);", # Index for new field
             """
            CREATE TABLE IF NOT EXISTS cluster_nodes (
                node_id TEXT PRIMARY KEY,            -- Akita Node ID
                node_name TEXT NOT NULL,
                cluster_name TEXT NOT NULL,
                address_hex TEXT,                    -- Last known RNS address hex
                last_seen REAL NOT NULL,             -- Unix timestamp
                is_leader BOOLEAN DEFAULT FALSE,
                resources TEXT,                      -- JSON string of resource info (cpu, mem, etc.)
                status TEXT DEFAULT 'online'         -- e.g., online, offline, degraded
                -- Add columns for capabilities and task count if they need persistence:
                -- capabilities TEXT,              -- JSON list of strings
                -- current_task_count INTEGER DEFAULT 0 
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_cluster_nodes_cluster_name ON cluster_nodes (cluster_name);",
            "CREATE INDEX IF NOT EXISTS idx_cluster_nodes_last_seen ON cluster_nodes (last_seen);",
        ]
        log.info("Queueing database schema initialization/verification...")
        
        # Queue all schema statements for execution
        final_future = Future() # Future to represent completion of the whole batch
        for i, stmt in enumerate(schema_statements):
            current_future = self.execute(stmt)
            # Link the final_future to the completion of the *last* statement queued
            if i == len(schema_statements) - 1:
                def _link_future(completed_future, target_future):
                    # Propagate result or exception from the last DDL future to the final_future
                    if completed_future.cancelled():
                        target_future.cancel()
                    elif completed_future.exception() is not None:
                        target_future.set_exception(completed_future.exception())
                    else:
                        target_future.set_result(completed_future.result())
                # Add callback to the future of the last statement
                current_future.add_done_callback(lambda cf, tf=final_future: _link_future(cf, tf))

        # Add a callback to log the final outcome of schema initialization
        def log_schema_init_done(f):
            if f.exception():
                log.error(f"Database schema initialization failed: {f.exception()}")
            else:
                log.info("Database schema initialization/verification completed successfully.")
        final_future.add_done_callback(log_schema_init_done)
        
        return final_future # Return the future representing the completion of the last statement


    def close(self):
        """Cleans up the database manager, stopping the worker thread gracefully."""
        # Check if already closed or not initialized
        if not getattr(self, '_initialized', False) or self._db_thread_stop_event.is_set():
            log.info("DatabaseManager already closed or not fully initialized.")
            return

        log.info("Attempting to close DatabaseManager...")
        # Send a sentinel value to the queue to signal the thread to stop after processing pending items
        stop_future = Future()
        self._db_queue.put(("STOP_THREAD_SENTINEL", (), None, stop_future))
        
        try:
            # Wait for the sentinel to be processed, ensuring queue is likely empty.
            # Use a timeout to prevent waiting indefinitely if something goes wrong.
            stop_future.result(timeout=10.0) 
            log.info("DB processing thread acknowledged stop sentinel.")
        except Exception as e: # Catches TimeoutError from future.result or other exceptions
            log.warning(f"DB thread did not process stop sentinel in time or error: {e}. Forcing stop event.")
            # Force stop if sentinel wasn't processed
            self._db_thread_stop_event.set() 

        # Ensure the stop event is set regardless of sentinel outcome
        self._db_thread_stop_event.set() 
        # Wait for the database thread to actually terminate
        self._db_thread.join(timeout=5.0) 
        
        if self._db_thread.is_alive():
            log.warning("Database processing thread did not stop in time after join().")
        else:
            log.info("Database processing thread stopped successfully.")
        
        # Clear any remaining items in the queue if thread was force-stopped or didn't process sentinel
        while not self._db_queue.empty():
            try:
                self._db_queue.get_nowait()
            except queue.Empty:
                break
            self._db_queue.task_done()
        log.info("DatabaseManager closed.")

# --- Global Instance & Cleanup ---
# Create the singleton instance when the module is imported
db_manager = DatabaseManager(settings.SQLITE_DB_FILE)

# Register the close method to be called automatically when the Python interpreter exits
import atexit
atexit.register(db_manager.close)

