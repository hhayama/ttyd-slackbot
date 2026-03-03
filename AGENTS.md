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
