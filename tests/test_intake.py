"""Tests for the Intake subsystem (Slack message receipt and guardrails)."""

from unittest.mock import MagicMock, patch

from ttyd_slackbot.engine import EngineResult
from ttyd_slackbot.intake.slack_app import _handle_message


def test_handle_message_logs_user_text_and_replies_with_success_when_guardrails_pass():
    """Handler receives user message, runs guardrails; when allowed, sends placeholder and engine response via output layer."""
    event = {"text": "What is total revenue?", "channel": "C123", "ts": "1234567890.123456"}
    mock_say = MagicMock()
    guardrail_result = {
        "allowed": True,
        "reason": None,
        "interpreted_query": "Total revenue from payments (SUM amount_usd).",
        "raw_query": "What is total revenue?",
    }
    mock_agent = MagicMock()
    engine_result = EngineResult(response_type="text", value="The total revenue is $42,000.")
    with patch("ttyd_slackbot.intake.slack_app.logger") as mock_logger, patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_or_create_agent_for_thread",
        return_value=mock_agent,
    ), patch(
        "ttyd_slackbot.intake.slack_app.run_query",
        return_value=engine_result,
    ) as mock_run_query:
        _handle_message(event, mock_say, None)
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0]
    assert "What is total revenue?" in call_args[1]
    assert mock_say.call_count == 2
    first_args, first_kw = mock_say.call_args_list[0]
    assert "There are no issues with your query" in first_args[0]
    assert "Total revenue from payments" in first_args[0]
    assert first_kw.get("thread_ts") == "1234567890.123456"
    second_args, second_kw = mock_say.call_args_list[1]
    assert "42,000" in second_args[0]
    assert second_kw.get("thread_ts") == "1234567890.123456"
    mock_run_query.assert_called_once_with(
        mock_agent, "What is total revenue?", is_follow_up=False
    )


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
