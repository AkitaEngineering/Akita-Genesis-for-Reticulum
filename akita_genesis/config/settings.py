# akita_genesis/config/settings.py
import os
from pathlib import Path
from typing import Optional, List, Set

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class AppSettings(BaseSettings):
    """
    Application settings for Akita Genesis.
    Settings can be loaded from a .env file, environment variables, or defaults.
    Environment variable names are derived from field names (e.g., AKITA_APP_NAME_SHORT).
    """
    model_config = SettingsConfigDict(
        env_prefix='AKITA_',  # All environment variables should start with AKITA_
        env_file=os.path.join(BASE_DIR, '.env'),
        env_file_encoding='utf-8',
        extra='ignore'  # Ignore extra fields from .env or environment
    )

    # General Application Settings
    APP_NAME_SHORT: str = Field(default="akitagen", description="Short name for Reticulum app identification, max 10 chars, no spaces/special chars.")
    APP_VERSION: str = Field(default="0.1.0-alpha", description="Application version.") # Added version
    LOG_LEVEL: str = Field(default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    SQLITE_DB_FILE: Path = Field(default=BASE_DIR / "akita_genesis_data" / "akita_ledger.db", description="Path to the SQLite database file.")
    DATA_DIR: Path = Field(default=BASE_DIR / "akita_genesis_data", description="Directory to store persistent data like identities and DB.")

    # Node & Cluster Settings
    DEFAULT_NODE_NAME: Optional[str] = Field(default=None, description="Default node name if not provided via CLI. If None, a random one might be generated or hostname used.")
    DEFAULT_CLUSTER_NAME: str = Field(default="default_cluster", description="Default cluster name if not specified via CLI.")
    CLUSTER_SIZE: int = Field(default=3, ge=1, description="Minimum desired number of nodes to form a functional cluster.")
    NODE_TIMEOUT_S: int = Field(default=180, ge=30, description="Seconds before a non-responsive node is considered stale or offline.")
    LEADER_ELECTION_TIMEOUT_S: int = Field(default=15, ge=5, description="Timeout for leader election rounds.")

    # Reticulum & Network Settings
    DISCOVERY_INTERVAL_S: int = Field(default=30, ge=5, description="Interval (in seconds) for node discovery broadcasts.")
    ANNOUNCE_INTERVAL_S: int = Field(default=60, ge=10, description="Interval (in seconds) for node presence announcements.")
    RNS_APP_NAME: str = Field(default="akita.genesis", description="Reticulum application name (used internally by RNS, not the short name).")
    DEFAULT_RNS_PORT: Optional[int] = Field(default=None, description="Default port for Reticulum Transport instance. None means Reticulum default. Use for testing multiple nodes on one host.")
    
    # Control API Settings (for CLI communication with nodes)
    DEFAULT_API_HOST: str = Field(default="0.0.0.0", description="Default host for the node's control API.")
    DEFAULT_API_PORT: int = Field(default=8000, ge=1024, le=65535, description="Default port for the node's control API.")
    CONTROL_API_TIMEOUT_S: int = Field(default=10, description="Timeout for API requests from CLI to node.")
    # --- NEW: API Security ---
    # Use SecretStr to prevent accidental logging of keys
    # Set via environment variable AKITA_VALID_API_KEYS='key1,key2' or in .env
    VALID_API_KEYS: Set[SecretStr] = Field(default_factory=set, description="Set of valid API keys for accessing the node's API.")
    API_KEY_HEADER_NAME: str = Field(default="X-API-Key", description="HTTP Header name for the API key.")

    # Task Management Settings
    TASK_PROCESSING_INTERVAL_S: int = Field(default=5, ge=1, description="Interval for the leader to check and process tasks from the queue.")
    MAX_TASK_QUEUE_SIZE: int = Field(default=1000, description="Maximum number of tasks in the pending queue.")
    DEFAULT_TASK_PRIORITY: int = Field(default=10, description="Default priority for tasks if not specified.")
    # --- NEW: Task Fault Tolerance ---
    MAX_TASK_EXECUTION_ATTEMPTS: int = Field(default=3, ge=1, description="Maximum times a task will be attempted before being marked FAILED.")
    WORKER_ACK_TIMEOUT_S: int = Field(default=30, ge=5, description="Seconds leader waits for a worker to acknowledge a task assignment.")
    WORKER_PROCESSING_TIMEOUT_S: int = Field(default=300, ge=10, description="Seconds leader waits for a worker to return a result after acknowledgment.")

    # Resource Monitoring
    RESOURCE_MONITOR_INTERVAL_S: int = Field(default=60, ge=10, description="Interval for updating and broadcasting resource metrics.")

    # Make sure data directory exists
    def __init__(self, **values):
        super().__init__(**values)
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        if self.SQLITE_DB_FILE.parent != self.DATA_DIR :
             self.SQLITE_DB_FILE.parent.mkdir(parents=True, exist_ok=True)

# Singleton instance of settings
settings = AppSettings()

# Validate app name short
if not settings.APP_NAME_SHORT.isalnum() or not (1 <= len(settings.APP_NAME_SHORT) <= 10):
    raise ValueError("APP_NAME_SHORT must be 1-10 alphanumeric characters.")

# Log if API keys are not set (potential security warning)
if not settings.VALID_API_KEYS:
    # Use the logger setup by utils if available, otherwise print
    try:
        # Attempt relative import suitable for when package is installed/used
        from ..utils.logger import log 
        log.warning("AKITA_VALID_API_KEYS is not set. The node's API will be unsecured.")
    except ImportError:
         # Fallback for direct execution or if logger isn't set up yet
         print("WARNING: AKITA_VALID_API_KEYS is not set. The node's API will be unsecured.")
