"""
Package containing all pre-defined pipeline stages
"""

# pragma pylint: disable=wildcard-import

import flags

from .base import BaseStage, When

from .batch import ByteBatchStage, RowBatchStage
from .custom import CustomActionStage, CustomCommandStage, CustomQueryStage
from .database import CreateTableStage, DropTableStage, PostImportHackStage
from .database import SourceComparisonStage, TruncateTableStage
from .misc import DelayStage, PromptInputStage, SkipImportStage
from .sources import FilterColumnsStage, JumbleDataStage, LimitImportStage
from .stream import StreamGenerationStage


__all__ = ["base", "batch", "custom", "database", "misc", "sources"]
