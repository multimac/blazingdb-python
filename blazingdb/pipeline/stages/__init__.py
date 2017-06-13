"""
Package containing all pre-defined pipeline stages
"""

# pragma pylint: disable=wildcard-import

import flags

from .base import BaseStage

from .custom import CustomActionStage, CustomCommandStage, CustomQueryStage
from .database import CreateTableStage, DropTableStage, TruncateTableStage
from .misc import DelayStage, PromptInputStage, PrefixTableStage
from .sources import FilterColumnsStage, JumbleDataStage, LimitImportStage


class When(flags.Flags):
    """ Defines the stages at which a custom query can be executed """

    before = ()
    after = ()

__all__ = ["base", "custom", "database", "misc", "sources"]
