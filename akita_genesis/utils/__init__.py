# akita_genesis/utils/__init__.py
# Initialization for the utils module.

# Make key utility functions/objects easily importable
try:
    from .logger import setup_logger, log
except ImportError:
     # Handle potential import issues during setup or testing
    pass

# You could add other utility imports here, e.g.:
# from .helpers import some_helper_function
