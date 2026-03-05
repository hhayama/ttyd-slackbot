"""Tests for the Intake subsystem (Slack message receipt)."""

from unittest.mock import MagicMock, patch

from ttyd_slackbot.intake.slack_app import _handle_message


def test_handle_message_logs_user_text_and_replies():
    """Handler receives user message event, logs the text, and sends placeholder reply."""
    event = {"text": "What is total revenue?", "channel": "C123"}
    mock_say = MagicMock()
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0]
    assert "What is total revenue?" in call_args[1]
    mock_say.assert_called_once_with("Thank you, we'll look into it!")


def test_handle_message_ignores_bot_messages():
    """Handler does not log or reply when event is from a bot."""
    event = {"text": "bot reply", "bot_id": "B123", "channel": "C123"}
    mock_say = MagicMock()
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_not_called()
    mock_say.assert_not_called()


def test_handle_message_ignores_bot_message_subtype():
    """Handler does not log or reply for subtype bot_message."""
    event = {"text": "hi", "subtype": "bot_message", "channel": "C123"}
    mock_say = MagicMock()
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_not_called()
    mock_say.assert_not_called()
