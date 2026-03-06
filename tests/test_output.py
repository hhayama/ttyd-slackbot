"""Tests for the Output subsystem (PII check, table formatting, prepare_for_slack)."""

from unittest.mock import patch

import pandas as pd
import pytest

from ttyd_slackbot.engine import EngineResult
from ttyd_slackbot.output.format_table import format_table_for_slack
from ttyd_slackbot.output.pii_check import PII_BLOCK_MESSAGE, check_pii
from ttyd_slackbot.output.prepare import prepare_for_slack


def test_pii_check_blocks_email():
    """check_pii returns safe=False and block message when email is present."""
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii("Contact us at support@example.com for help.", use_llm=False)
    assert result["safe"] is False
    assert result["output"] == PII_BLOCK_MESSAGE


def test_pii_check_blocks_phone():
    """check_pii returns safe=False when phone number is present."""
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii("Call 555-123-4567 for details.", use_llm=False)
    assert result["safe"] is False
    assert result["output"] == PII_BLOCK_MESSAGE


def test_pii_check_allows_safe_text():
    """check_pii returns safe=True and same text when no PII detected."""
    text = "The total revenue is $42,000."
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii(text, use_llm=False)
    assert result["safe"] is True
    assert result["output"] == text


def test_pii_check_blocks_ssn():
    """check_pii returns safe=False when SSN pattern is present."""
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii("SSN: 123-45-6789", use_llm=False)
    assert result["safe"] is False
    assert result["output"] == PII_BLOCK_MESSAGE


def test_format_table_empty_dataframe():
    """format_table_for_slack returns no-rows message for empty DataFrame."""
    df = pd.DataFrame()
    out = format_table_for_slack(df)
    assert "No rows" in out


def test_format_table_produces_markdown():
    """format_table_for_slack produces markdown table with header and rows."""
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = format_table_for_slack(df)
    assert "| " in out
    assert " a " in out or "a" in out
    assert " b " in out or "b" in out
    assert "1" in out and "2" in out
    assert "x" in out and "y" in out


def test_format_table_truncates_rows():
    """format_table_for_slack truncates to max_rows and adds note."""
    df = pd.DataFrame({"x": range(100)})
    out = format_table_for_slack(df, max_rows=5)
    assert "Showing first 5 of 100" in out
    assert out.count("|") >= 5 * 2  # header + sep + 5 rows


def test_prepare_for_slack_text_passes_through_when_safe():
    """prepare_for_slack returns engine text when PII check passes."""
    engine_result = EngineResult(response_type="text", value="Total count is 100.")
    with patch("ttyd_slackbot.output.prepare.check_pii", return_value={"safe": True, "output": "Total count is 100."}):
        out = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert out == "Total count is 100."


def test_prepare_for_slack_text_blocks_when_pii():
    """prepare_for_slack returns PII block message when check fails."""
    engine_result = EngineResult(response_type="text", value="Email: user@test.com")
    with patch("ttyd_slackbot.output.prepare.check_pii", return_value={"safe": False, "output": PII_BLOCK_MESSAGE}):
        out = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert out == PII_BLOCK_MESSAGE


def test_prepare_for_slack_table_formats_then_checks_pii():
    """prepare_for_slack formats table then runs PII check on formatted string."""
    df = pd.DataFrame({"col": [1, 2, 3]})
    engine_result = EngineResult(response_type="table", value=df)
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "| col |\n| --- |\n| 1 |\n| 2 |\n| 3 |"}
        out = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert "|" in out
    mock_check.assert_called_once()
    call_arg = mock_check.call_args[0][0]
    assert "col" in call_arg and "1" in call_arg


def test_prepare_for_slack_number_converts_to_string():
    """prepare_for_slack converts number response to string and checks PII."""
    engine_result = EngineResult(response_type="number", value=42.5)
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "42.5"}
        out = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert out == "42.5"
    mock_check.assert_called_once_with("42.5", messages=[], interpreted_query=None, use_llm=True)
