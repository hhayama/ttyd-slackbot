"""
In-memory conversation memory for Intake.

Thread-scoped store keyed by (channel_id, thread_ts) so follow-up messages
in the same Slack thread have full conversation context for the LLM.
"""

from typing import Any

# Key: (channel_id, thread_ts); value: list of {"role": "user"|"assistant", "content": str}
_thread_messages: dict[tuple[str, str], list[dict[str, Any]]] = {}


def get_thread_key(channel_id: str, thread_ts: str) -> tuple[str, str]:
    """Return the storage key for a thread (channel_id, thread_ts)."""
    return (channel_id, thread_ts)


def get_messages(channel_id: str, thread_ts: str) -> list[dict[str, Any]]:
    """
    Return the list of messages for the given thread.

    Returns a copy so callers can append without mutating the stored list
    until they call append_message.
    """
    key = get_thread_key(channel_id, thread_ts)
    return list(_thread_messages.get(key, []))


def append_message(channel_id: str, thread_ts: str, role: str, content: str) -> None:
    """Append a message to the thread history. role is 'user' or 'assistant'."""
    key = get_thread_key(channel_id, thread_ts)
    if key not in _thread_messages:
        _thread_messages[key] = []
    _thread_messages[key].append({"role": role, "content": content})
