"""
Defines a series of pipeline stages for performing custom actions, including:
 - CustomActionStage
"""

from . import base, When


class CustomActionStage(base.PipelineStage):
    """ Performs a custom callback before / after importing data """

    def __init__(self, callback, *packets, **kwargs):
        super(CustomActionStage, self).__init__(*packets)
        self.when = kwargs.get("when", When.before)
        self.callback = callback

    async def _perform_callback(self, message):
        return await self.callback(message)

    async def before(self, message):
        """ Triggers the callback if it has queued to run before the import """
        if When.before not in self.when:
            return

        return await self._perform_callback(message)

    async def after(self, message):
        """ Triggers the callback if it has queued to run after the import """
        if When.after not in self.when:
            return

        return await self._perform_callback(message)
