"""
Help intent detection: lightweight check for "what can I ask" / help requests.

Used in intake before guardrails so we can reply with the pre-saved help
response without calling the LLM or engine.
"""

import re

# Phrases that indicate the user wants to know what can be queried or needs help.
# Case-insensitive; prefer phrases to avoid triggering on "what's the total revenue".
HELP_PATTERNS = [
    r"\bhelp\b",
    r"what\s+can\s+I\s+ask",
    r"what\s+can\s+I\s+query",
    r"what\s+data\s+(do\s+you\s+have|is\s+available)",
    r"what('s|s)\s+available",
    r"what\s+do\s+you\s+have",
    r"what\s+can\s+you\s+do",
    r"what\s+tables",
    r"what\s+can\s+be\s+queried",
    r"not\s+sure\s+what\s+to\s+ask",
    r"show\s+me\s+the\s+schema",
    r"what\s+questions\s+(can\s+I\s+)?ask",
    r"\bcapabilities\b",
    r"what\s+do\s+you\s+know",
    r"list\s+tables",
    r"available\s+data",
    r"what\s+kind\s+of\s+(questions|queries)",
    r"what\s+can\s+I\s+get",
]
HELP_REGEX = re.compile("|".join(f"({p})" for p in HELP_PATTERNS), re.IGNORECASE)


def is_help_intent(text: str) -> bool:
    """
    Return True if the message indicates the user wants help or to know what can be queried.

    Uses a regex allowlist of phrases; no LLM. Intended for short messages like
    "help", "what can I ask?", "not sure what to query".

    Parameters
    ----------
    text : str
        The latest user message (e.g. from Slack).

    Returns
    -------
    bool
        True if we should respond with the pre-saved help response.
    """
    if not text or not text.strip():
        return False
    return HELP_REGEX.search(text.strip()) is not None
