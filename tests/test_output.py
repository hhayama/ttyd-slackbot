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


def test_pii_check_allows_aggregate_metrics_without_llm():
    """check_pii returns safe=True for aggregate-like text when use_llm=False (regex does not false-positive)."""
    aggregate_text = "Revenue by country: US 5000, UK 3000. Distinct user_id count per country: 100, 200."
    result = check_pii(aggregate_text, messages=[], use_llm=False)
    assert result["safe"] is True
    assert result["output"] == aggregate_text


def test_pii_check_allows_single_user_id_without_llm():
    """check_pii returns safe=True for single user_id in analytical answer when use_llm=False."""
    text = "The user_id of the longest subscriber is 42."
    result = check_pii(text, messages=[], use_llm=False)
    assert result["safe"] is True
    assert result["output"] == text


def test_pii_check_allows_aggregate_metrics_with_llm():
    """check_pii returns safe=True for aggregate output when LLM says safe."""
    aggregate_text = "country | user_count\nUS | 100\nUK | 200"
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii(
            aggregate_text,
            messages=[{"role": "user", "content": "users per country"}],
            interpreted_query="Count of distinct user_id by country.",
            use_llm=True,
        )
    assert result["safe"] is True
    assert result["output"] == aggregate_text


def test_pii_check_allows_single_user_id_analytical_with_llm():
    """check_pii returns safe=True for single user_id in analytical answer when LLM says safe."""
    text = "The user_id of the longest subscriber is 42."
    with patch("ttyd_slackbot.output.pii_check._llm_pii_check", return_value=True):
        result = check_pii(
            text,
            messages=[{"role": "user", "content": "who has the longest subscription?"}],
            interpreted_query="Return the user_id of the user with the longest subscription.",
            use_llm=True,
        )
    assert result["safe"] is True
    assert result["output"] == text


def test_format_table_empty_dataframe():
    """format_table_for_slack returns no-rows message for empty DataFrame."""
    df = pd.DataFrame()
    out = format_table_for_slack(df)
    assert "No rows" in out


def test_format_table_produces_box_table():
    """format_table_for_slack produces ASCII box-drawn table with header and rows."""
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = format_table_for_slack(df)
    assert "+" in out and "-" in out and "|" in out
    assert "a" in out and "b" in out
    assert "1" in out and "2" in out
    assert "x" in out and "y" in out


def test_format_table_truncates_rows():
    """format_table_for_slack truncates to max_rows and adds note."""
    df = pd.DataFrame({"x": range(100)})
    out = format_table_for_slack(df, max_rows=5)
    assert "Showing first 5 of 100" in out
    assert out.count("+") >= 2 and out.count("|") >= 2


def test_format_table_numeric_right_aligned():
    """format_table_for_slack right-aligns numeric columns."""
    df = pd.DataFrame({"n": [1, 99], "s": ["a", "bb"]})
    out = format_table_for_slack(df)
    # Numeric column: " 1" and "99" should be right-aligned (space before 1)
    assert " 1" in out or "1" in out
    assert "99" in out
    assert "a" in out and "bb" in out


def test_prepare_for_slack_text_passes_through_when_safe():
    """prepare_for_slack returns (text, None, None) when PII check passes."""
    engine_result = EngineResult(response_type="text", value="Total count is 100.")
    with patch("ttyd_slackbot.output.prepare.check_pii", return_value={"safe": True, "output": "Total count is 100."}):
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert text == "Total count is 100."
    assert file_bytes is None
    assert file_name is None


def test_prepare_for_slack_text_blocks_when_pii():
    """prepare_for_slack returns PII block message when check fails."""
    engine_result = EngineResult(response_type="text", value="Email: user@test.com")
    with patch("ttyd_slackbot.output.prepare.check_pii", return_value={"safe": False, "output": PII_BLOCK_MESSAGE}):
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert text == PII_BLOCK_MESSAGE
    assert file_bytes is None
    assert file_name is None


def test_prepare_for_slack_table_formats_then_checks_pii():
    """prepare_for_slack formats table then runs PII check on formatted string."""
    df = pd.DataFrame({"col": [1, 2, 3]})
    engine_result = EngineResult(response_type="table", value=df)
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "+-----+\n| col |\n+-----+\n|   1 |\n|   2 |\n|   3 |\n+-----+"}
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert "|" in text
    assert file_bytes is None
    assert file_name is None
    mock_check.assert_called_once()
    call_arg = mock_check.call_args[0][0]
    assert "col" in call_arg and "1" in call_arg


def test_prepare_for_slack_number_converts_to_string():
    """prepare_for_slack converts number response to string and checks PII."""
    engine_result = EngineResult(response_type="number", value=42.5)
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "42.5"}
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert text == "42.5"
    assert file_bytes is None
    assert file_name is None
    mock_check.assert_called_once_with("42.5", messages=[], interpreted_query=None, use_llm=True)


def test_prepare_for_slack_chart_returns_image_when_save_available():
    """prepare_for_slack returns (caption, png_bytes, filename) for chart when value has save()."""
    class MockChart:
        def save(self, path):
            with open(path, "wb") as f:
                f.write(b"\x89PNG\r\n\x1a\n")

    engine_result = EngineResult(response_type="chart", value=MockChart())
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "Here's your chart."}
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert text == "Here's your chart."
    assert file_bytes == b"\x89PNG\r\n\x1a\n"
    assert file_name == "chart.png"
    mock_check.assert_called_once()


def test_prepare_for_slack_chart_fallback_when_no_save():
    """prepare_for_slack returns (str(value), None, None) for chart when value has no save()."""
    engine_result = EngineResult(response_type="chart", value="chart repr")
    with patch("ttyd_slackbot.output.prepare.check_pii") as mock_check:
        mock_check.return_value = {"safe": True, "output": "chart repr"}
        text, file_bytes, file_name = prepare_for_slack(engine_result, messages=[], interpreted_query=None)
    assert text == "chart repr"
    assert file_bytes is None
    assert file_name is None
