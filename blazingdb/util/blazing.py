"""
Utility methods for BlazingDB
"""


DATATYPE_BUILDERS = {
    "date": "date", "datetime": "date",

    "bool": "bool",
    "float": "float", "long": "long",

    "str": "string({size})"
}

def build_datatype(column):
    """ Builds the data type identifier for the given column """
    format_str = DATATYPE_BUILDERS.get(column.type)
    return format_str.format(**column._asdict())
