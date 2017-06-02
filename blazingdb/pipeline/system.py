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

    async def __aenter__(self):
        try:
            await self._process_begin()
        except:
            await self._process_end()
            raise

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._process_end()

    async def _process_begin(self):
        for stage in self.pipeline:
            await stage.begin_import(self.data)
            self.processed.append(stage)

    async def _process_end(self):
        errored = False
        while self.processed:
            try:
                stage = self.processed.pop()
                await stage.end_import(self.data)
            except Exception:  # pylint: disable=broad-except
                self.logger.exception("Exception occurred while processing end_import")

                errored = True

        if errored:
            raise exceptions.PipelineException()
