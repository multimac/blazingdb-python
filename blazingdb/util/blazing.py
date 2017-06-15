"""
Utility methods for BlazingDB
"""

import datetime


# pragma pylint: disable=multiple-statements

DATE_FORMAT = "%Y-%m-%d"

DATATYPES = [
    "bool", "date", "float", "double",
    "char", "short", "int", "long",
    "string"
]

def _parse_float(value): return float(value)
def _parse_int(value): return int(value)
def _parse_string(value): return str(value)

def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"

    raise TypeError("Could not parse {0} as bool".format(value))

def _parse_date(value):
    if isinstance(value, datetime.date):
        return value
    elif isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, str):
        return datetime.date.strptime(DATE_FORMAT)

    raise TypeError("Could not parse {0} as date".format(value))


DATATYPE_BUILDERS = {
    **{datatype: datatype for datatype in DATATYPES},

    "string": "string({size})"
}
DATATYPE_PARSERS = {
    "bool": _parse_bool, "date": _parse_date,

    "float": _parse_float, "double": _parse_float,

    "char": _parse_int, "short": _parse_int,
    "int": _parse_int, "long": _parse_int,

    "string": _parse_string
}

assert all(k in DATATYPE_PARSERS for k in DATATYPES)
assert all(k in DATATYPE_BUILDERS for k in DATATYPES)


def build_datatype(column):
    format_str = DATATYPE_BUILDERS.get(column.type, column.type)
    return format_str.format(**column._asdict())

def parse_value(datatype, value):
    return DATATYPE_PARSERS[datatype](value)
