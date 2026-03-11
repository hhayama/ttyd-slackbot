"""Tests for help intent detection."""

import pytest

from ttyd_slackbot.intake.help_intent import is_help_intent


@pytest.mark.parametrize(
    "text",
    [
        "help",
        "Help",
        "HELP",
        "what can I ask?",
        "What can I ask",
        "what can I query",
        "what data do you have",
        "what's available",
        "what do you have",
        "what can you do",
        "what tables",
        "what can be queried",
        "not sure what to ask",
        "show me the schema",
        "what questions can I ask",
        "capabilities",
        "what do you know",
        "list tables",
        "available data",
        "what kind of questions",
        "what can I get",
    ],
)
def test_is_help_intent_true(text):
    """Phrases indicating help or 'what can I query' return True."""
    assert is_help_intent(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "What is total revenue?",
        "What's the total revenue",
        "show me payments by method",
        "how many users signed up last month",
        "",
        "   ",
    ],
)
def test_is_help_intent_false(text):
    """Normal data questions and empty text return False."""
    assert is_help_intent(text) is False


def test_is_help_intent_none_equivalent():
    """Falsy input is treated as no help intent."""
    assert is_help_intent("") is False
    assert is_help_intent("   ") is False
