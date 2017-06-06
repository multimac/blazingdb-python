"""
Defines classes involved in running stages of a pipeline
"""

import logging

from .. import exceptions

class System(object):  # pylint: disable=too-few-public-methods
    """ Wraps an array of pipeline stages """

    def __init__(self, pipeline=None):
        if pipeline is None:
            pipeline = []

        self.pipeline = pipeline

    def process(self, data=None):
        return SystemContext(self.pipeline, data)


class SystemContext(object):  # pylint: disable=too-few-public-methods
    """ A context manager to handle running begin/end import methods """

    def __init__(self, pipeline, data):
        self.logger = logging.getLogger(__name__)

        self.pipeline = pipeline
        self.data = data

        self.processed = []

    async def _process_begin(self):
        for stage in self.pipeline:
            await stage.before(self.data)
            self.processed.append(stage)

    async def _process_end(self, success):
        data = {"success": success}.update(self.data)

        errors = []
        while self.processed:
            stage = self.processed.pop()
            try:
                await stage.after(data)
            except Exception as ex:  # pylint: disable=broad-except
                errors.append(ex)

        if errors:
            raise exceptions.PipelineException(errors)

    async def __aenter__(self):
        try:
            await self._process_begin()
        except:
            await self._process_end(False)
            raise

        return

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._process_end(exc_val is None)
