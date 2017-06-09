""""
Defines tests for the custom pipeline sources.
"""

import datetime
import unittest

from blazingdb.pipeline import sources
from blazingdb.sources import base

class TestSource(base.BaseSource):
    """ Test source fed into pipeline sources """

    def __init__(self):
        self.data = {
            "one": {
                "columns": [("a", "long"), ("b", "date"), ("c", "string(256)")],
                "data": [
                    (0, datetime.date(2017, 1, 1), "Barry Boy"),
                    (1, datetime.date(2010, 12, 25), "Jackie Smith"),
                    (2, datetime.date(2010, 1, 1), "John Doe")
                ]
            }, "two": {
                "columns": [("a", "double"), ("b", "string(32)")],
                "data": [
                    (100.0, "Top"), (74.99, "Middle"),
                    (42.0, "Meaning of life"), (22.5, "Twenty-two point five")
                ]
            }
        }

    def get_tables(self):
        return self.data.keys()

    def get_columns(self, table):
        return [
            {"name": item[0], "type": item[1]}
            for item in self.data[table]["columns"]
        ]

    def retrieve(self, table):
        yield from self.data[table]["data"]

class JumbledSourceTests(unittest.TestCase):
    """ Tests for JumbledSource """

    def setUp(self):
        self.test_source = TestSource()
        self.source = sources.JumbledSource(self.test_source)

    def test_rows_become_jumbled(self):
        """ JumbledSource.retrieve should return rows with different data (of the correct types) """
        for row_index, row in enumerate(self.source.retrieve("one")):
            data = self.test_source.data["one"]["data"][row_index]

            self.assertEqual(len(row), len(data))
            for col_index, col in enumerate(data):
                self.assertIsInstance(row[col_index], type(col))
                self.assertNotEqual(row[col_index], col)

if __name__ == "__main__":
    unittest.main()
