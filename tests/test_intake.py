"""Tests for the Intake subsystem (Slack message receipt and guardrails)."""

from unittest.mock import MagicMock, patch

from ttyd_slackbot.intake.slack_app import _handle_message


def test_handle_message_logs_user_text_and_replies_with_success_when_guardrails_pass():
    """Handler receives user message, runs guardrails; when allowed, sends success message and repeats query."""
    event = {"text": "What is total revenue?", "channel": "C123", "ts": "1234567890.123456"}
    mock_say = MagicMock()
    guardrail_result = {
        "allowed": True,
        "reason": None,
        "interpreted_query": "Total revenue from payments (SUM amount_usd).",
    }
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger, patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ):
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0]
    assert "What is total revenue?" in call_args[1]
    mock_say.assert_called_once()
    args, kwargs = mock_say.call_args
    assert "There are no issues with your query" in args[0]
    assert "Total revenue from payments" in args[0]
    assert kwargs.get("thread_ts") == "1234567890.123456"


def test_handle_message_when_guardrails_block_says_reason():
    """When guardrails return allowed=False, handler says the reason and does not repeat query."""
    event = {"text": "What are user emails?", "channel": "C123", "ts": "1234567890.123456"}
    mock_say = MagicMock()
    guardrail_result = {
        "allowed": False,
        "reason": "We cannot answer questions about PII such as emails.",
        "interpreted_query": None,
    }
    with patch("ttyd_slackbot.intake.slack_app.logger"), patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ):
        _handle_message(event, mock_say, None)
    mock_say.assert_called_once_with(
        "We cannot answer questions about PII such as emails.",
        thread_ts="1234567890.123456",
    )


def test_handle_message_ignores_bot_messages():
    """Handler does not log or reply when event is from a bot."""
    event = {"text": "bot reply", "bot_id": "B123", "channel": "C123", "ts": "1.1"}
    mock_say = MagicMock()
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_not_called()
    mock_say.assert_not_called()


def test_handle_message_ignores_bot_message_subtype():
    """Handler does not log or reply for subtype bot_message."""
    event = {"text": "hi", "subtype": "bot_message", "channel": "C123", "ts": "1.1"}
    mock_say = MagicMock()
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_not_called()
    mock_say.assert_not_called()
