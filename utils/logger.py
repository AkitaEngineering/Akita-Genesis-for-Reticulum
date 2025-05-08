# akita_genesis/utils/logger.py
import logging
import sys
import os

# Attempt to import settings safely, handle potential circularity during setup
try:
    from ..config.settings import settings
    DEFAULT_LOG_LEVEL = settings.LOG_LEVEL
except ImportError:
    # Fallback if settings cannot be imported (e.g., during early setup)
    DEFAULT_LOG_LEVEL = os.environ.get("AKITA_LOG_LEVEL", "INFO") 

# Ensure the logger is configured only once per name to avoid duplicate handlers
_loggers_configured = {}

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

            # TODO: Add File Handler based on settings if needed
            # log_file_path = getattr(settings, 'LOG_FILE_PATH', None)
            # if log_file_path:
            #     file_handler = logging.FileHandler(log_file_path)
            #     file_handler.setFormatter(formatter)
            #     logger.addHandler(file_handler)

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

# Create a default 'root' logger instance for the application for easy import
# Components can then get their own child logger: log = logging.getLogger(__name__)
# which will inherit the configuration from this root logger.
log = setup_logger("akita_genesis", level=DEFAULT_LOG_LEVEL)

# --- Example Usage in other modules ---
# import logging
# log = logging.getLogger(__name__) # Get child logger named after the module
# log.info("This message will use the root handler configuration.")
#
# Or, if specific configuration is needed for a module:
# from .utils.logger import setup_logger
# module_log = setup_logger(__name__, level="DEBUG") # Configure specifically
# module_log.debug("A specific debug message.")

