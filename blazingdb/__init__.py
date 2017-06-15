"""
Package for handling connections to BlazingDB from Python
"""

from . import exceptions

from .connector import Connector
from .migrator import Migrator

if __name__ == "__main__":
    from . import main
    import sys

    sys.exit(main.main())


__all__ = ["connector", "exceptions", "importers", "migrator", "pipeline", "sources", "triggers"]
