#!/usr/bin/env python3
"""
Verify Option A: PandasAI v3 resolves ${DB_HOST}, ${DB_USER}, etc. when loading schema.yaml.

Run from project root with .env (or DB_* / DATABASE_URL) set:
  poetry run python scripts/verify_pandasai_load.py

If this succeeds, PandasAI is resolving placeholders from the environment when connecting.
"""

import os
import sys
from pathlib import Path

# Project root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)

def main():
    from dotenv import load_dotenv
    load_dotenv()

    if not os.environ.get("DB_HOST") and not os.environ.get("DATABASE_URL"):
        print("Set DB_HOST (and DB_NAME, DB_USER, DB_PASSWORD) or DATABASE_URL in .env", file=sys.stderr)
        sys.exit(1)

    import pandasai as pai

    try:
        obj = pai.load("ttyd/sessions")
        print("pai.load('ttyd/sessions') OK")
        # Optional: trigger a trivial query to confirm connection uses resolved credentials
        if hasattr(obj, "chat"):
            out = obj.chat("How many rows are there?")
            print("agent.chat('How many rows are there?') ->", out)
    except Exception as e:
        print("Verification failed:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
