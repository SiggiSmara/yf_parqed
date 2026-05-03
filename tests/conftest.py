import re


def strip_ansi(text: str) -> str:
    """Strip ANSI escape sequences from CLI output before asserting on content."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)
