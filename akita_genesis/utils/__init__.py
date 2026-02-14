# akita_genesis/utils/__init__.py
# Initialization for the utils module.

# Make key utility functions/objects easily importable
from .logger import setup_logger, log, get_recent_logs

__all__ = ["setup_logger", "log", "get_recent_logs"]

# You could add other utility imports here, e.g.:
# from .helpers import some_helper_function
