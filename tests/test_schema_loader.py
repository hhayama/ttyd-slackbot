"""Tests for the Intake schema loader."""

from pathlib import Path

import pytest

from ttyd_slackbot.intake.schema_loader import get_schema_summary


def test_get_schema_summary_returns_non_empty_from_datasets_dir(tmp_path):
    """get_schema_summary finds schema.yaml files and returns a non-empty string."""
    # Create minimal org/dataset/schema.yaml layout
    org = "ttyd"
    (tmp_path / org / "users").mkdir(parents=True)
    (tmp_path / org / "users" / "schema.yaml").write_text(
        "name: users\ndescription: Users table\ncolumns:\n  - name: user_id\n    type: integer\n    description: id\n"
    )
    summary = get_schema_summary(datasets_dir=tmp_path, org=org)
    assert summary
    assert "users" in summary
    assert "user_id" in summary
    assert "integer" in summary


def test_get_schema_summary_includes_aliases(tmp_path):
    """Schema summary includes column alias/aliases when present."""
    org = "ttyd"
    (tmp_path / org / "payments").mkdir(parents=True)
    (tmp_path / org / "payments" / "schema.yaml").write_text(
        "name: payments\ndescription: Payments\ncolumns:\n"
        "  - name: method\n    type: string\n    description: payment method\n    aliases:\n      - apple_pay\n"
    )
    summary = get_schema_summary(datasets_dir=tmp_path, org=org)
    assert "method" in summary
    assert "apple_pay" in summary or "aliases" in summary.lower()


def test_get_schema_summary_empty_when_org_missing(tmp_path):
    """get_schema_summary returns empty string when org directory does not exist."""
    summary = get_schema_summary(datasets_dir=tmp_path, org="nonexistent")
    assert summary == ""
