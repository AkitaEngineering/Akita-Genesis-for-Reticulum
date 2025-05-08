# akita_genesis/config/__init__.py
# Initialization for the config module.

# Make the settings object easily importable from the config package
try:
    from .settings import settings
except ImportError:
    # Handle cases where settings might not be importable yet (e.g., during setup)
    # Or if there's a circular dependency issue being resolved.
    pass
