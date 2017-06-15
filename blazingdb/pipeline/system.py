"""
Defines classes involved in running stages of a pipeline
"""

import functools
import logging

from .stages import base


# pragma pylint: disable=too-few-public-methods

class System(object):
    """ Wraps an array of pipeline stages """

    def __init__(self, stages=None):
        if stages is None:
            stages = []

        self.stages = stages

    def process(self, data=None):
        return SystemContext(self.stages, data)


class SystemContext(object):
    """ A context manager to handle running begin/end import methods """

    def __init__(self, stages, data):
        self.logger = logging.getLogger(__name__)

        self.pipeline = self._build(stages, data)

    @staticmethod
    def _build(stages, data):
        async def _call_step(step, old_data, data=None):
            data = data if data is not None else dict()

            next_data = {
                k: v for k, v
                in {**old_data, **data}.items()
                if v is not None
            }

            async for item in step(next_data):
                yield item

        async def _chain_stage(func, step, data):
            next_step = functools.partial(_call_step, step, data)

            async for item in func(next_step, data):
                yield item

        final_stage = GeneratorStage()
        step = functools.partial(_chain_stage, final_stage.process, None)

        for stage in reversed(stages):
            step = functools.partial(_chain_stage, stage.process, step)

        return functools.partial(step, data)

    async def __aiter__(self):
        return self.pipeline()

class GeneratorStage(base.BaseStage):
    """ Final stage which yields the given data object """

    async def process(self, _, data):
        yield data
