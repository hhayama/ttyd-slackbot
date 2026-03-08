"""Tests for the Intake LLM guardrails."""

from unittest.mock import MagicMock, patch

from ttyd_slackbot.intake.guardrails import check_guardrails


def test_check_guardrails_parses_allowed_true_and_interpreted_query():
    """When OpenAI returns valid JSON with allowed=true, parsed result has interpreted_query."""
    messages = [{"role": "user", "content": "What is total revenue?"}]
    schema = "Dataset: payments\n  - amount_usd (float): amount in USD\n"
    fake_content = '{"allowed": true, "reason": null, "interpreted_query": "Total revenue from payments (SUM amount_usd)."}'
    with patch("ttyd_slackbot.intake.guardrails.os.environ", {"OPENAI_API_KEY": "test-key"}), patch(
        "ttyd_slackbot.intake.guardrails.OpenAI"
    ) as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(message=MagicMock(content=fake_content))
            ]
        )
        result = check_guardrails(messages, schema)
    assert result["allowed"] is True
    assert result["reason"] is None
    assert "Total revenue" in (result["interpreted_query"] or "")


def test_check_guardrails_blocks_pii_terms_via_regex_no_llm_call():
    """When the user message contains a blocked PII term (e.g. email), regex blocks and OpenAI is not called."""
    messages = [{"role": "user", "content": "What are user email addresses?"}]
    with patch("ttyd_slackbot.intake.guardrails.OpenAI") as mock_openai_class:
        result = check_guardrails(messages, "Dataset: users\n")
    assert result["allowed"] is False
    assert result["reason"] is not None
    assert "personal or contact" in result["reason"].lower() or "PII" in result["reason"]
    assert result["interpreted_query"] is None
    assert result["raw_query"] == "What are user email addresses?"
    mock_openai_class.assert_not_called()


def test_check_guardrails_blocks_ssn_and_drivers_license_via_regex():
    """Blocked terms like ssn and driver's license trigger regex block without calling LLM."""
    for content in ["Show me SSN for users", "I need driver's license numbers"]:
        messages = [{"role": "user", "content": content}]
        with patch("ttyd_slackbot.intake.guardrails.OpenAI") as mock_openai_class:
            result = check_guardrails(messages, "Dataset: users\n")
        assert result["allowed"] is False, f"Expected block for: {content!r}"
        assert result["reason"] is not None
        assert result["interpreted_query"] is None
        mock_openai_class.assert_not_called()


def test_check_guardrails_invalid_json_returns_not_allowed():
    """When the model returns non-JSON, guardrails return allowed=False with generic reason."""
    messages = [{"role": "user", "content": "Hello"}]
    with patch("ttyd_slackbot.intake.guardrails.os.environ", {"OPENAI_API_KEY": "test-key"}), patch(
        "ttyd_slackbot.intake.guardrails.OpenAI"
    ) as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(message=MagicMock(content="I'm not JSON at all."))
            ]
        )
        result = check_guardrails(messages, "")
    assert result["allowed"] is False
    assert result["reason"] is not None
    assert result["interpreted_query"] is None


def test_check_guardrails_missing_api_key_returns_not_allowed():
    """When OPENAI_API_KEY is not set, returns allowed=False with reason."""
    messages = [{"role": "user", "content": "What is revenue?"}]
    with patch("ttyd_slackbot.intake.guardrails.os.environ", {"OPENAI_API_KEY": ""}):
        result = check_guardrails(messages, "Dataset: payments\n")
    assert result["allowed"] is False
    assert result["reason"] is not None
