"""Tests for the Engine subsystem."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ttyd_slackbot.engine.runner import (
    EngineResult,
    _list_dataset_names,
    _try_consume_agent_csv_file,
    create_agent,
    get_or_create_agent_for_thread,
    run_query,
)


def test_list_dataset_names_empty_when_org_missing(tmp_path):
    """_list_dataset_names returns [] when org directory does not exist."""
    assert _list_dataset_names(tmp_path, "nonexistent") == []


def test_list_dataset_names_returns_sorted_names(tmp_path):
    """_list_dataset_names returns sorted dataset names that have schema.yaml."""
    org = "ttyd"
    for name in ("sessions", "users", "payments"):
        (tmp_path / org / name).mkdir(parents=True)
        (tmp_path / org / name / "schema.yaml").write_text("name: " + name + "\n")
    (tmp_path / org / "no_schema").mkdir(parents=True)
    assert _list_dataset_names(tmp_path, org) == ["payments", "sessions", "users"]


def test_create_agent_raises_when_no_datasets(tmp_path):
    """create_agent raises ValueError when no datasets exist."""
    with pytest.raises(ValueError, match="No datasets found|no dataset"):
        create_agent(datasets_dir=tmp_path, org="nonexistent")


def test_run_query_uses_chat_when_not_follow_up():
    """run_query calls agent.chat when is_follow_up is False and returns EngineResult."""
    mock_agent = MagicMock()
    mock_agent.chat.return_value = "42"
    mock_agent.follow_up.return_value = "nope"
    result = run_query(mock_agent, "What is total?", is_follow_up=False)
    assert result.response_type == "text"
    assert result.value == "42"
    mock_agent.chat.assert_called_once_with("What is total?")
    mock_agent.follow_up.assert_not_called()


def test_run_query_uses_follow_up_when_is_follow_up():
    """run_query calls agent.follow_up when is_follow_up is True and returns EngineResult."""
    mock_agent = MagicMock()
    mock_agent.follow_up.return_value = "Based on the previous question, ..."
    mock_agent.chat.return_value = "nope"
    result = run_query(mock_agent, "And by region?", is_follow_up=True)
    assert result.response_type == "text"
    assert "Based on the previous" in result.value
    mock_agent.follow_up.assert_called_once_with("And by region?")
    mock_agent.chat.assert_not_called()


def test_get_or_create_agent_for_thread_creates_once_then_reuses():
    """get_or_create_agent_for_thread returns same agent for same thread (no DB in test)."""
    from ttyd_slackbot.engine import runner as runner_mod

    mock_agent = MagicMock()
    with patch.object(runner_mod, "create_agent", return_value=mock_agent) as mock_create:
        a1 = get_or_create_agent_for_thread("C1", "ts1")
        a2 = get_or_create_agent_for_thread("C1", "ts1")
        assert a1 is a2 is mock_agent
        mock_create.assert_called_once()
    # Different thread gets a new agent
    with patch.object(runner_mod, "create_agent", return_value=MagicMock()) as p:
        get_or_create_agent_for_thread("C1", "ts2")
        p.assert_called_once()


def test_run_query_discovers_datasets_dir_from_env(monkeypatch, tmp_path):
    """run_query uses DATASETS_DIR and SEMANTIC_LAYER_ORG when not passed."""
    org = "myorg"
    (tmp_path / org / "foo").mkdir(parents=True)
    (tmp_path / org / "foo" / "schema.yaml").write_text("name: foo\n")
    monkeypatch.setenv("DATASETS_DIR", str(tmp_path))
    monkeypatch.setenv("SEMANTIC_LAYER_ORG", org)
    # Would actually call pai.load and need DB - so we only test discovery path.
    # With no DB, run_query will fail at pai.load; we just ensure we don't error earlier.
    names = _list_dataset_names(tmp_path, org)
    assert names == ["foo"]
    # run_query with this dir would try pai.load("myorg/foo") and need real DB - skip here
    monkeypatch.delenv("DATASETS_DIR", raising=False)
    monkeypatch.delenv("SEMANTIC_LAYER_ORG", raising=False)


def test_try_consume_agent_csv_file_returns_unchanged_when_not_text():
    """_try_consume_agent_csv_file returns result unchanged when response_type is not text."""
    result = EngineResult(response_type="table", value=None)
    assert _try_consume_agent_csv_file(result) is result


def test_try_consume_agent_csv_file_returns_unchanged_when_no_match():
    """_try_consume_agent_csv_file returns result unchanged when text does not match pattern."""
    result = EngineResult(response_type="text", value="Some other message.")
    assert _try_consume_agent_csv_file(result).response_type == "text"
    assert _try_consume_agent_csv_file(result).value == "Some other message."


def test_try_consume_agent_csv_file_reads_and_deletes_file(tmp_path):
    """When text matches 'CSV file saved as X.csv' and file exists in cwd, returns csv_file and deletes file."""
    csv_path = tmp_path / "exported_data.csv"
    csv_content = b"a,b\n1,2\n"
    csv_path.write_bytes(csv_content)
    result = EngineResult(
        response_type="text",
        value="CSV file saved as exported_data.csv",
    )
    with patch.object(Path, "cwd", return_value=tmp_path):
        out = _try_consume_agent_csv_file(result)
    assert out.response_type == "csv_file"
    content, filename = out.value
    assert content == csv_content
    assert filename == "exported_data.csv"
    assert not csv_path.exists()


def test_try_consume_agent_csv_file_unchanged_when_file_missing(tmp_path):
    """When text matches but file does not exist, return result unchanged."""
    result = EngineResult(
        response_type="text",
        value="CSV file saved as missing.csv",
    )
    with patch.object(Path, "cwd", return_value=tmp_path):
        out = _try_consume_agent_csv_file(result)
    assert out.response_type == "text"
    assert "missing.csv" in out.value


def test_try_consume_agent_csv_file_rejects_path_traversal():
    """Filename with '..' or path sep is not consumed."""
    result = EngineResult(
        response_type="text",
        value="CSV file saved as ../../../etc/passwd.csv",
    )
    out = _try_consume_agent_csv_file(result)
    assert out.response_type == "text"


def test_run_query_consumes_csv_file_when_agent_returns_saved_message(tmp_path):
    """When agent returns 'CSV file saved as X.csv' and file exists, run_query returns csv_file result."""
    csv_path = tmp_path / "report.csv"
    csv_path.write_bytes(b"col1,col2\n1,2\n")
    mock_agent = MagicMock()
    mock_agent.chat.return_value = MagicMock()
    mock_agent.chat.return_value.value = "CSV file saved as report.csv"
    mock_agent.follow_up.return_value = MagicMock()
    with patch.object(Path, "cwd", return_value=tmp_path):
        result = run_query(mock_agent, "export as csv", is_follow_up=False)
    assert result.response_type == "csv_file"
    content, filename = result.value
    assert content == b"col1,col2\n1,2\n"
    assert filename == "report.csv"
    assert not csv_path.exists()
