# akita_genesis/__init__.py
# This file makes Python treat the directory as a package.
# You can also define package-level variables or import specific modules here.
import os
from pathlib import Path

# Try to determine version from settings if available, otherwise fallback
try:
    from .config.settings import settings
    __version__ = settings.APP_VERSION
except ImportError:
     # Fallback version if settings cannot be imported yet (e.g., during setup)
     # Keep this in sync with setup.py and settings.py
    __version__ = "0.1.0-alpha" 

__author__ = "Akita Engineering"
__email__ = "info@akitaengineering.com"

# Define the base directory of the package
PACKAGE_BASE_DIR = Path(__file__).resolve().parent


