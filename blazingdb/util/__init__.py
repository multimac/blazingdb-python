"""
Package containing utility modules
"""


def exponential_moving_avg(alpha=0.5):
    """ Calculates an exponential moving average """
    current = average = yield

    while True:
        average = current * alpha + average * (1 - alpha)
        current = yield average

def format_size(size, suffix="B"):
    """ Formats the given size as a human-readable string """
    format_str = "%.1f%s%s"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(size) < 1024:
            return format_str % (size, unit, suffix)

        size /= 1024

    return format_str % (size, "Yi", suffix)


__all__ = ["timer"]
