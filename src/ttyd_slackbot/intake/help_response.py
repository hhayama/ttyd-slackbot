"""
Pre-saved help response: what tables exist and what can be queried.

Builds user-facing content from the semantic layer schema (same source as
schema_loader). Loads from a saved file if present; otherwise generates from
schema and returns (no LLM or engine).
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

HELP_RESPONSE_FILENAME = "help_response.md"
NO_DATASETS_MESSAGE = "No datasets are configured yet."


def _get_datasets_dir_and_org(datasets_dir: Path | None = None, org: str = "ttyd") -> tuple[Path, str]:
    """Resolve datasets root and org; same convention as schema_loader."""
    if datasets_dir is None:
        base = os.environ.get("DATASETS_DIR")
        datasets_dir = Path(base).resolve() if base else Path.cwd() / "datasets"
    else:
        datasets_dir = datasets_dir.resolve()
    return datasets_dir, org


def build_help_content(datasets_dir: Path | None = None, org: str = "ttyd") -> str:
    """
    Build user-facing help text from schema YAMLs under datasets/<org>/<name>/.

    Returns a short intro and per-dataset: name, description, and what can be
    asked (column names/descriptions). If no datasets exist, returns
    NO_DATASETS_MESSAGE.

    Parameters
    ----------
    datasets_dir : Path or None
        Root datasets directory. Defaults to DATASETS_DIR env or cwd/datasets.
    org : str
        Organization name (e.g. "ttyd").

    Returns
    -------
    str
        Human-readable help content for Slack.
    """
    root, org_name = _get_datasets_dir_and_org(datasets_dir, org)
    org_path = root / org_name
    if not org_path.is_dir():
        return NO_DATASETS_MESSAGE

    parts = [
        "Here’s what you can ask about. I have access to the following datasets:",
        "",
    ]
    count = 0
    for schema_path in sorted(org_path.iterdir()):
        if not schema_path.is_dir():
            continue
        yaml_path = schema_path / "schema.yaml"
        if not yaml_path.exists():
            continue
        with open(yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not data:
            continue
        count += 1
        name = data.get("name", schema_path.name)
        desc = data.get("description", "")
        parts.append(f"*{name}*")
        if desc:
            parts.append(desc)
        columns = data.get("columns") or []
        if columns:
            part_cols = "You can ask about: " + ", ".join(
                col.get("name", "") for col in columns if col.get("name")
            )
            parts.append(part_cols)
        parts.append("")

    if count == 0:
        return NO_DATASETS_MESSAGE
    return "\n".join(parts).strip()


def get_help_response_path(datasets_dir: Path | None = None, org: str = "ttyd") -> Path:
    """Return the path where help_response.md is stored (or should be written)."""
    root, org_name = _get_datasets_dir_and_org(datasets_dir, org)
    return root / org_name / HELP_RESPONSE_FILENAME


def load_help_response(datasets_dir: Path | None = None, org: str = "ttyd") -> str:
    """
    Load help content: read from file if it exists, else build from schema.

    Uses the same datasets_dir/org convention as schema_loader. If the file
    is missing, falls back to build_help_content() and logs that the file
    can be created.

    Parameters
    ----------
    datasets_dir : Path or None
        Root datasets directory.
    org : str
        Organization name.

    Returns
    -------
    str
        Help text to send to the user.
    """
    path = get_help_response_path(datasets_dir, org)
    if path.exists():
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError as e:
            logger.warning("Could not read help response file %s: %s", path, e)
    else:
        logger.info(
            "Help response file not found at %s; using schema-generated content. "
            "Run scripts/generate_help_response.py to create it.",
            path,
        )
    return build_help_content(datasets_dir, org)
