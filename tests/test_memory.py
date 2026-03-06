"""Tests for the Intake in-memory conversation store."""

from ttyd_slackbot.intake.memory import append_message, get_messages, get_thread_key


def test_get_thread_key():
    """Thread key is (channel_id, thread_ts)."""
    assert get_thread_key("C1", "123.456") == ("C1", "123.456")


def test_append_and_get_messages():
    """Appending messages and getting messages returns the correct history."""
    channel_id = "C1"
    thread_ts = "123.0"
    assert get_messages(channel_id, thread_ts) == []
    append_message(channel_id, thread_ts, "user", "What is revenue?")
    append_message(channel_id, thread_ts, "assistant", "Total revenue is X.")
    msgs = get_messages(channel_id, thread_ts)
    assert len(msgs) == 2
    assert msgs[0]["role"] == "user" and msgs[0]["content"] == "What is revenue?"
    assert msgs[1]["role"] == "assistant" and msgs[1]["content"] == "Total revenue is X."


def test_get_messages_returns_copy():
    """get_messages returns a copy so mutating the list does not affect stored history."""
    channel_id = "C2"
    thread_ts = "456.0"
    append_message(channel_id, thread_ts, "user", "Hi")
    msgs = get_messages(channel_id, thread_ts)
    msgs.append({"role": "user", "content": "fake"})
    assert len(get_messages(channel_id, thread_ts)) == 1
