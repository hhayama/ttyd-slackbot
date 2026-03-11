#!/usr/bin/env python3
"""
Generate and save the help response file from the semantic layer schema.

Writes datasets/<org>/help_response.md (default org: ttyd). Run from project root:
  poetry run python scripts/generate_help_response.py

Uses DATASETS_DIR env if set; otherwise cwd/datasets.
"""

import argparse
import os
import sys
from pathlib import Path

# Project root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and save help_response.md from schema")
    parser.add_argument(
        "--org",
        default=os.environ.get("HELP_RESPONSE_ORG", "ttyd"),
        help="Organization name under datasets/ (default: ttyd)",
    )
    parser.add_argument(
        "--datasets-dir",
        type=Path,
        default=Path(os.environ.get("DATASETS_DIR", "datasets")).resolve(),
        help="Root datasets directory",
    )
    args = parser.parse_args()

    from ttyd_slackbot.intake.help_response import (
        build_help_content,
        get_help_response_path,
    )

    content = build_help_content(datasets_dir=args.datasets_dir, org=args.org)
    path = get_help_response_path(datasets_dir=args.datasets_dir, org=args.org)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"Wrote {path}", file=sys.stderr)
    print(f"Length: {len(content)} chars", file=sys.stderr)


if __name__ == "__main__":
    main()
