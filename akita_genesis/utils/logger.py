# akita_genesis/utils/logger.py
import importlib
import logging
import sys
import os
import time
from typing import Dict, Any, Optional

DEFAULT_LOG_LEVEL = os.environ.get("AKITA_LOG_LEVEL", "INFO")
loaded_settings: Any = None

# Attempt to import settings safely, handle potential circularity during setup
try:
    settings_module = importlib.import_module("akita_genesis.config.settings")
    loaded_settings = getattr(settings_module, "settings", None)
    DEFAULT_LOG_LEVEL = str(getattr(loaded_settings, "LOG_LEVEL", DEFAULT_LOG_LEVEL))
except ImportError:
    # Fallback if settings cannot be imported (e.g., during early setup)
    pass

# Ensure the logger is configured only once per name to avoid duplicate handlers
_loggers_configured: Dict[str, bool] = {}

def setup_logger(name: str = "akita_genesis", level: str = DEFAULT_LOG_LEVEL) -> logging.Logger:
    """
    Sets up a standardized logger instance.

    Avoids adding multiple handlers if called multiple times for the same logger name.
    Child loggers inherit level and handlers from the root logger unless configured separately.

    Args:
        name (str): The name for the logger (e.g., 'akita_genesis', 'akita_genesis.module.submodule').
        level (str): The logging level string (e.g., "INFO", "DEBUG").

    Returns:
        logging.Logger: Configured logger instance.
    """
    global _loggers_configured
    
    # Use the provided level, fallback to default if necessary
    effective_level_str = level.upper()
    log_level = getattr(logging, effective_level_str, logging.INFO)

    logger = logging.getLogger(name)

    # Configure only if this specific logger hasn't been configured before
    if name not in _loggers_configured:
        logger.setLevel(log_level)
        
        # Prevent adding handlers if logger already has them (e.g., from root config)
        if not logger.hasHandlers():
            # Console Handler setup
            console_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                # Example format: 2023-10-27 10:30:00 - [INFO] - akita_genesis.core.node (node.py.<module>:42) - Message here
                "%(asctime)s - [%(levelname)s] - %(name)s (%(filename)s.%(funcName)s:%(lineno)d) - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # Add File Handler based on settings if needed
            log_file_path = getattr(loaded_settings, 'LOG_FILE_PATH', None)
            if log_file_path:
                file_handler = logging.FileHandler(log_file_path)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        # Mark this logger name as configured
        _loggers_configured[name] = True
        
        # Ensure messages propagate to parent loggers (like the root logger) if needed
        # Set propagate to False if you want to handle logging only at this specific logger level
        logger.propagate = True 

        print(f"Logger '{name}' configured with level {effective_level_str}.", file=sys.stderr) # Debug print during setup
    else:
         # If logger already configured, potentially update its level if different
         if logger.level != log_level:
              print(f"Updating logger '{name}' level to {effective_level_str}.", file=sys.stderr)
              logger.setLevel(log_level)


    return logger


# --- In-memory (ring buffer) log handler to support API/CLI fetching of recent logs ---
from collections import deque

class InMemoryLogHandler(logging.Handler):
    """Stores recent log records in memory (thread-safe for single-process use).

    Entries are stored as dictionaries with keys: timestamp (float), level, logger, message, module, funcName, lineno.
    The buffer size can be configured via the AKITA_LOG_HISTORY environment variable (default 1000).
    """
    def __init__(self, capacity: Optional[int] = None):
        super().__init__()
        try:
            capacity = int(os.environ.get("AKITA_LOG_HISTORY", "1000")) if capacity is None else int(capacity)
        except Exception:
            capacity = 1000
        self._buf: 'deque[Dict[str, Any]]' = deque(maxlen=capacity)
        # Use a simple formatter for the stored message text
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            entry = {
                "timestamp": time.time(),
                "level": record.levelname,
                "logger": record.name,
                "message": msg,
                "module": record.module,
                "funcName": record.funcName,
                "lineno": record.lineno,
            }
            self._buf.append(entry)
        except Exception:
            self.handleError(record)

    def get_entries(self, limit: Optional[int] = None, level: Optional[str] = None):
        """Return recent entries (newest first)."""
        items = list(self._buf)
        if level:
            level = level.upper()
            items = [i for i in items if i.get("level") == level]
        if limit:
            return items[-int(limit):]
        return items


# Small module-level holder for the global in-memory handler so it isn't re-created repeatedly
_in_memory_log_handler: Optional[InMemoryLogHandler] = None


def _ensure_inmemory_handler_attached(logger: logging.Logger):
    global _in_memory_log_handler
    if _in_memory_log_handler is None:
        _in_memory_log_handler = InMemoryLogHandler()
    # Attach it once per logger if not present
    if not any(isinstance(h, InMemoryLogHandler) for h in logger.handlers):
        logger.addHandler(_in_memory_log_handler)


def get_recent_logs(limit: int = 100, level: str | None = None):
    """Return recent log entries from the in-memory buffer.

    This is a synchronous helper intended for use by the API and CLI.
    """
    global _in_memory_log_handler
    if not _in_memory_log_handler:
        return []
    entries = _in_memory_log_handler.get_entries(limit=limit, level=level)
    return entries


# Create a default 'root' logger instance for the application for easy import
# Components can then get their own child logger: log = logging.getLogger(__name__)
# which will inherit the configuration from this root logger.
root_logger = setup_logger("akita_genesis", level=DEFAULT_LOG_LEVEL)
# Ensure the in-memory handler is attached to the root logger as well
_ensure_inmemory_handler_attached(root_logger)
# Expose a convenient alias 'log' for backwards compatibility
log = root_logger

# --- Example Usage in other modules ---
# import logging
# log = logging.getLogger(__name__) # Get child logger named after the module
# log.info("This message will use the root handler configuration.")
#
# Or, if specific configuration is needed for a module:
# from .utils.logger import setup_logger
# module_log = setup_logger(__name__, level="DEBUG") # Configure specifically
# module_log.debug("A specific debug message.")
