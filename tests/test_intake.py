"""Tests for the Intake subsystem (Slack message receipt)."""

from unittest.mock import patch

from ttyd_slackbot.intake.slack_app import _handle_message


def test_handle_message_logs_user_text():
    """Handler receives user message event and logs the text."""
    event = {"text": "What is total revenue?", "channel": "C123"}
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, None)
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0]
    assert "What is total revenue?" in call_args[1]


def test_handle_message_ignores_bot_messages():
    """Handler does not log when event is from a bot."""
    event = {"text": "bot reply", "bot_id": "B123", "channel": "C123"}
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, None)
    mock_logger.info.assert_not_called()


def test_handle_message_ignores_bot_message_subtype():
    """Handler does not log for subtype bot_message."""
    event = {"text": "hi", "subtype": "bot_message", "channel": "C123"}
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, None)
    mock_logger.info.assert_not_called()
