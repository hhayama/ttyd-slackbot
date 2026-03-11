"""Tests for the Intake subsystem (Slack message receipt and guardrails)."""

from unittest.mock import MagicMock, patch

from ttyd_slackbot.engine import EngineResult
from ttyd_slackbot.intake.slack_app import _handle_message


def test_handle_message_replies_with_output_layer_only_when_guardrails_pass():
    """When guardrails pass on a new thread, handler sends initial message then engine result."""
    event = {
        "text": "What is total revenue?",
        "channel": "C123",
        "ts": "1234567890.123456",
        "user": "U42",
    }
    mock_say = MagicMock()
    mock_context = MagicMock()
    mock_context.client.users_info.return_value = {
        "user": {"profile": {"display_name": "Jane"}, "real_name": "Jane Doe"},
    }
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
    ) as mock_run_query, patch(
        "ttyd_slackbot.intake.slack_app.append_message",
    ):
        _handle_message(event, mock_say, mock_context)
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0]
    assert "What is total revenue?" in call_args[1]
    assert mock_say.call_count == 1
    first_say_args, first_say_kw = mock_say.call_args_list[0]
    assert "Hi!" in first_say_args[0] and "Jane" in first_say_args[0]
    assert "loading the data" in first_say_args[0] and "looking into it" in first_say_args[0]
    assert first_say_kw.get("thread_ts") == "1234567890.123456"
    mock_context.client.chat_update.assert_called_once()
    update_kw = mock_context.client.chat_update.call_args[1]
    assert "42,000" in update_kw["text"]
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


def test_handle_message_help_intent_returns_saved_response_without_guardrails_or_engine():
    """When user asks for help (what can I ask), reply with help content and skip guardrails/engine."""
    event = {
        "text": "what can I ask?",
        "channel": "C123",
        "ts": "1234567890.123456",
    }
    mock_say = MagicMock()
    help_content = "Here's what you can ask about. I have access to: *users* ..."
    with patch("ttyd_slackbot.intake.slack_app.is_help_intent", return_value=True), patch(
        "ttyd_slackbot.intake.slack_app._get_help_response", return_value=help_content
    ), patch("ttyd_slackbot.intake.slack_app.check_guardrails") as mock_guard, patch(
        "ttyd_slackbot.intake.slack_app.get_or_create_agent_for_thread"
    ) as mock_agent, patch(
        "ttyd_slackbot.intake.slack_app.run_query"
    ) as mock_run:
        _handle_message(event, mock_say, None)
    mock_say.assert_called_once_with(help_content, thread_ts="1234567890.123456")
    mock_guard.assert_not_called()
    mock_agent.assert_not_called()
    mock_run.assert_not_called()


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


def test_handle_message_uploads_chart_via_files_upload_v2():
    """When prepare_for_slack returns file_bytes and file_name, handler calls context.client.files_upload_v2 with correct args."""
    event = {"text": "chart that", "channel": "C99", "ts": "111.222", "user": "U1"}
    mock_say = MagicMock()
    mock_upload = MagicMock()
    mock_context = MagicMock()
    mock_context.client.files_upload_v2 = mock_upload
    mock_context.client.users_info.return_value = {
        "user": {"profile": {"display_name": "Alex"}, "real_name": "Alex Smith"},
    }
    guardrail_result = {
        "allowed": True,
        "reason": None,
        "interpreted_query": "Bar chart of data.",
        "raw_query": "chart that",
    }
    chart_bytes = b"\x89PNG\r\n\x1a\n"
    caption = "Here's your chart."
    with patch("ttyd_slackbot.intake.slack_app.logger"), patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_or_create_agent_for_thread",
        return_value=MagicMock(),
    ), patch(
        "ttyd_slackbot.intake.slack_app.run_query",
        return_value=EngineResult(response_type="chart", value=None),
    ), patch(
        "ttyd_slackbot.intake.slack_app.prepare_for_slack",
        return_value=(caption, chart_bytes, "chart.png"),
    ), patch("ttyd_slackbot.intake.slack_app.append_message"):
        _handle_message(event, mock_say, mock_context)
    mock_upload.assert_called_once_with(
        channel="C99",
        content=chart_bytes,
        filename="chart.png",
        thread_ts="111.222",
    )
    assert mock_say.call_count == 1
    say_calls = [c[0][0] for c in mock_say.call_args_list]
    assert "Hi!" in say_calls[0] and "Alex" in say_calls[0]
    mock_context.client.chat_update.assert_called_once()
    assert caption in mock_context.client.chat_update.call_args[1]["text"]


def test_handle_message_follow_up_no_initial_message():
    """When guardrails pass on a follow-up, handler sends only the engine result (no initial greeting)."""
    event = {
        "text": "And last month?",
        "channel": "C123",
        "ts": "1234567890.123456",
        "user": "U42",
    }
    mock_say = MagicMock()
    guardrail_result = {
        "allowed": True,
        "reason": None,
        "interpreted_query": "Same for last month.",
        "raw_query": "And last month?",
    }
    mock_agent = MagicMock()
    engine_result = EngineResult(response_type="text", value="Last month revenue was $38,000.")
    # Thread already has user + assistant messages, so is_follow_up is True
    messages_with_history = [
        {"role": "user", "content": "What is total revenue?"},
        {"role": "assistant", "content": "The total revenue is $42,000."},
        {"role": "user", "content": "And last month?"},
    ]
    with patch("ttyd_slackbot.intake.slack_app.logger"), patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_messages",
        return_value=messages_with_history,
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_or_create_agent_for_thread",
        return_value=mock_agent,
    ), patch(
        "ttyd_slackbot.intake.slack_app.run_query",
        return_value=engine_result,
    ), patch("ttyd_slackbot.intake.slack_app.append_message"):
        _handle_message(event, mock_say, None)
    assert mock_say.call_count == 2
    say_args = mock_say.call_args_list[1][0][0]
    assert "38,000" in say_args


def test_handle_message_initial_message_uses_fallback_name_when_context_none():
    """When context is None, initial message uses fallback 'there' and still sends initial + result."""
    event = {
        "text": "What is total revenue?",
        "channel": "C999",
        "ts": "9999999999.999999",
        "user": "U42",
    }
    mock_say = MagicMock()
    guardrail_result = {
        "allowed": True,
        "reason": None,
        "interpreted_query": "Total revenue.",
        "raw_query": "What is total revenue?",
    }
    engine_result = EngineResult(response_type="text", value="The total revenue is $42,000.")
    # Ensure new-thread semantics: only one user message (no prior assistant)
    fresh_messages = [{"role": "user", "content": "What is total revenue?"}]
    with patch("ttyd_slackbot.intake.slack_app.logger"), patch(
        "ttyd_slackbot.intake.slack_app.check_guardrails", return_value=guardrail_result
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_messages",
        return_value=fresh_messages,
    ), patch(
        "ttyd_slackbot.intake.slack_app.get_or_create_agent_for_thread",
        return_value=MagicMock(),
    ), patch(
        "ttyd_slackbot.intake.slack_app.run_query",
        return_value=engine_result,
    ), patch("ttyd_slackbot.intake.slack_app.append_message"):
        _handle_message(event, mock_say, None)
    assert mock_say.call_count == 2
    first_say_args = mock_say.call_args_list[0][0][0]
    assert "there" in first_say_args and "Hi!" in first_say_args
    second_say_args = mock_say.call_args_list[1][0][0]
    assert "42,000" in second_say_args
