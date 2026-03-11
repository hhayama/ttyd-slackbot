"""Tests for help response builder and loader."""

from pathlib import Path

import pytest

from ttyd_slackbot.intake.help_response import (
    NO_DATASETS_MESSAGE,
    build_help_content,
    get_help_response_path,
    load_help_response,
)


def test_build_help_content_returns_non_empty_from_schema(tmp_path):
    """build_help_content finds schema and returns intro + dataset info."""
    org = "ttyd"
    (tmp_path / org / "users").mkdir(parents=True)
    (tmp_path / org / "users" / "schema.yaml").write_text(
        "name: users\ndescription: User signups and device.\ncolumns:\n"
        "  - name: user_id\n    type: integer\n    description: id\n"
    )
    content = build_help_content(datasets_dir=tmp_path, org=org)
    assert content
    assert "what you can ask" in content.lower() or "datasets" in content.lower()
    assert "users" in content
    assert "user_id" in content


def test_build_help_content_empty_org_returns_no_datasets_message(tmp_path):
    """When org directory does not exist, return NO_DATASETS_MESSAGE."""
    content = build_help_content(datasets_dir=tmp_path, org="nonexistent")
    assert content == NO_DATASETS_MESSAGE


def test_build_help_content_no_schema_yaml_in_org_returns_no_datasets(tmp_path):
    """When org exists but has no dataset dirs with schema.yaml, return NO_DATASETS_MESSAGE."""
    (tmp_path / "ttyd").mkdir(parents=True)
    content = build_help_content(datasets_dir=tmp_path, org="ttyd")
    assert content == NO_DATASETS_MESSAGE


def test_get_help_response_path_under_org(tmp_path):
    """get_help_response_path returns datasets/org/help_response.md."""
    path = get_help_response_path(datasets_dir=tmp_path, org="ttyd")
    assert path == tmp_path / "ttyd" / "help_response.md"


def test_load_help_response_reads_file_when_present(tmp_path):
    """When help_response.md exists, load_help_response returns its content."""
    org = "ttyd"
    (tmp_path / org).mkdir(parents=True)
    help_path = tmp_path / org / "help_response.md"
    help_path.write_text("Custom help text here.")
    content = load_help_response(datasets_dir=tmp_path, org=org)
    assert content == "Custom help text here."


def test_load_help_response_builds_from_schema_when_file_missing(tmp_path):
    """When file is missing, load_help_response falls back to build_help_content."""
    org = "ttyd"
    (tmp_path / org / "payments").mkdir(parents=True)
    (tmp_path / org / "payments" / "schema.yaml").write_text(
        "name: payments\ndescription: Payments.\ncolumns:\n  - name: amount\n    type: float\n    description: amount\n"
    )
    # Do not create help_response.md
    content = load_help_response(datasets_dir=tmp_path, org=org)
    assert content
    assert "payments" in content
    assert "amount" in content
