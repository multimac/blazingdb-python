"""
Package containing utility modules
"""


def format_size(size, suffix="B"):
    """ Formats the given size as a human-readable string """
    format_str = "%.1f%s%s"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(size) < 1024:
            return format_str % (size, unit, suffix)

        size /= 1024

    return format_str % (size, "Yi", suffix)


__all__ = ["timer"]
