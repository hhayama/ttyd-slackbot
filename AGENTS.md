## Project Guidelines
- Always propose a plan in numbered steps before editing code.
- Keep changes minimal and avoid overengineering.
- Remove temporary or large data files, never commit datasets.

## Conventions
- Python: PEP8 style, Ruff linting, NumPy-style docstrings.
- Use pytest for testing, prefer pandas/NumPy/scikit-learn for analysis.
- Jupyter notebooks must remain reproducible: no hard-coded paths, use relative paths.

## Agent Instructions
- Run `pytest -q` after changes and share results.
- Confirm before installing new dependencies.
- Never write secrets, always use environment variables.
- When using PandasAI in code use v3 functions and conventions.
- Engine: DB_* or DATABASE_URL must be set before running queries. PandasAI v3 resolves `${VAR}` in schema.yaml connection config from the environment (Option A). To verify: `poetry run python scripts/verify_pandasai_load.py`. 
