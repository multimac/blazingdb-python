"""
Package containing all classes related to building and running a pipeline
"""

import flags

from . import stages
from .system import System


__all__ = ["stages", "system"]
