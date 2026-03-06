"""
Load semantic layer schema YAMLs and build a summary string for guardrails.

Used by the LLM to decide if a user query is answerable from the available
datasets (guardrail 2). No database or PandasAI calls; YAML and filesystem only.
"""

import os
from pathlib import Path

import yaml


def get_schema_summary(datasets_dir: Path | None = None, org: str = "ttyd") -> str:
    """
    Discover schema.yaml files under datasets/<org>/<name>/ and build a summary string.

    For each dataset: name, description, and for each column: name, type,
    description, and any alias/aliases. Used by guardrails to validate that
    the user's question is answerable from the semantic layer.

    Parameters
    ----------
    datasets_dir : Path or None
        Root datasets directory. Defaults to DATASETS_DIR env or cwd/datasets.
    org : str
        Organization name (subfolder under datasets_dir), e.g. "ttyd".

    Returns
    -------
    str
        Human-readable summary of all datasets and columns for the LLM.
    """
    if datasets_dir is None:
        base = os.environ.get("DATASETS_DIR")
        datasets_dir = Path(base).resolve() if base else Path.cwd() / "datasets"
    else:
        datasets_dir = datasets_dir.resolve()

    org_path = datasets_dir / org
    if not org_path.is_dir():
        return ""

    parts = []
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
        name = data.get("name", schema_path.name)
        desc = data.get("description", "")
        parts.append(f"Dataset: {name}\nDescription: {desc}")
        columns = data.get("columns") or []
        for col in columns:
            cname = col.get("name", "")
            ctype = col.get("type", "")
            cdesc = col.get("description", "")
            alias = col.get("alias")
            aliases = col.get("aliases") or []
            alias_str = ""
            if alias:
                alias_str = f" (alias: {alias})"
            if aliases:
                alias_str = f" (aliases: {', '.join(aliases)})"
            parts.append(f"  - {cname} ({ctype}): {cdesc}{alias_str}")
        parts.append("")

    return "\n".join(parts).strip()
