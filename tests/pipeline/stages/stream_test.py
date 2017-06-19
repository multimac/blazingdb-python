"""
Unit tests for the StreamGenerationStage
"""

import datetime
import timeit
import unittest

from blazingdb import importers
from blazingdb.pipeline.stages import StreamGenerationStage

class StreamPerformanceTests(unittest.TestCase):
    """ Tests the performance of the StreamGenerationStage """

    @staticmethod
    def test_process_row():
        """ Tests the performance of the _process_row method """
        # pragma pylint: disable=protected-access
        stage = StreamGenerationStage()

        datatypes = ["long", "long", "string", "date"]
        row = [1, None, "A", datetime.date(2016, 1, 20)]
        row_format = importers.RowFormat(",", "\"", "\n")

        mappings = [stage._create_mapping(row_format, dt) for dt in datatypes]

        timer = timeit.Timer(lambda: stage._process_row(row_format, mappings, row))

        results = timer.timeit()
        per_second = timeit.default_number / results

        print("Rows processed per second:", int(per_second))
