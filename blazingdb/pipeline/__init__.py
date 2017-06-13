"""
Package containing all classes related to building and running a pipeline
"""

import flags

from .stages import *  # pylint: disable=wildcard-import
from .system import System


__all__ = ["stages", "system"]
