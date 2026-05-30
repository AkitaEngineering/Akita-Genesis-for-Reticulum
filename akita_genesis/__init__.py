# akita_genesis/__init__.py
# This file makes Python treat the directory as a package.
# You can also define package-level variables or import specific modules here.
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

PACKAGE_VERSION_FALLBACK = "0.1.0-alpha"

try:
    __version__ = version("akita_genesis")
except PackageNotFoundError:
    __version__ = PACKAGE_VERSION_FALLBACK

__author__ = "Akita Engineering"
__email__ = "info@akitaengineering.com"

# Define the base directory of the package
PACKAGE_BASE_DIR = Path(__file__).resolve().parent


